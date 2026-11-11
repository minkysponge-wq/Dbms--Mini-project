"""Generate the ER diagram, as a draw.io file and as a PNG.

Both outputs come from the one model declared below, so they cannot
drift apart:

    docs/er_diagram.drawio   open and edit at app.diagrams.net
    docs/er_diagram.png      the snapshot for the report

Modelling note
--------------
This is a proper ER model, not a pre-resolved relational one. Five
associations are drawn as RELATIONSHIPS (diamonds) rather than entity
boxes, because that is what they are:

    VOTE               ternary: TERM x NODE(voter) x NODE(candidate)
    REPLICATION_STATE  M:N, recursive on NODE (leader / follower)
    CLIENT_SESSION     M:N between NODE and CLIENT
    USER_ROLE          M:N between APP_USER and APP_ROLE
    ROLE_PERMISSION    M:N between APP_ROLE and APP_PERMISSION

Each becomes a relation when the model is mapped to the relational
schema, which is exactly the list the report's "relations that are not
entities" section needs. Drawing them as boxes from the start would
hide that step.

Convention: relationships that map to a relation of their own are drawn
as diamonds; simple 1:N relationships are labelled connectors carrying
their cardinality. With twelve entities, diamonds for every relationship
would be unreadable.
"""

from __future__ import annotations

import html
import os
import subprocess

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

# name -> (x, y, is_weak, [(attribute, kind)])  kind: pk | fk | pkfk | ''
ENTITIES: dict[str, tuple] = {
    "TERM": (40, 360, False, [
        ("term_number", "pk"), ("leader_id", "fk"),
        ("started_at", ""), ("ended_at", ""),
    ]),
    "NODE": (800, 300, False, [
        ("node_id", "pk"), ("host_address", ""), ("port", ""),
        ("role", ""), ("current_term", "fk"), ("voted_for", "fk"),
        ("commit_index", ""), ("last_applied", ""), ("is_alive", ""),
    ]),
    "LOG_ENTRY": (1560, 40, True, [
        ("node_id", "pkfk"), ("log_index", "pk"), ("term_number", "fk"),
        ("command_type", ""), ("key_name", ""), ("value_text", ""),
        ("client_id", ""), ("seq_no", ""), ("committed", ""),
    ]),
    "KV_STATE": (1560, 360, True, [
        ("node_id", "pkfk"), ("key_name", "pk"), ("value_text", ""),
        ("last_applied_index", ""), ("owner_user_id", "fk"),
        ("updated_at", ""),
    ]),
    "SNAPSHOT": (1560, 640, False, [
        ("snapshot_id", "pk"), ("node_id", "fk"),
        ("last_included_index", ""), ("last_included_term", ""),
        ("state_blob", ""),
    ]),
    "NODE_EVENT": (1560, 880, False, [
        ("event_id", "pk"), ("node_id", "fk"), ("event_type", ""),
        ("old_value", ""), ("new_value", ""), ("description", ""),
        ("occurred_at", ""),
    ]),
    "CLIENT": (800, 920, False, [
        ("client_id", "pk"), ("user_id", "fk"), ("opened_at", ""),
    ]),
    "CLIENT_REQUEST": (1560, 1140, False, [
        ("request_id", "pk"), ("client_id", "fk"), ("seq_no", ""),
        ("command_type", ""), ("assigned_index", "fk"), ("status", ""),
    ]),
    "APP_USER": (40, 1060, False, [
        ("user_id", "pk"), ("username", ""), ("email", ""),
        ("password_hash", ""), ("is_active", ""), ("created_at", ""),
    ]),
    "AUTH_SESSION": (40, 1400, False, [
        ("token_hash", "pk"), ("user_id", "fk"), ("issued_at", ""),
        ("expires_at", ""), ("revoked", ""),
    ]),
    "APP_ROLE": (800, 1320, False, [
        ("role_id", "pk"), ("role_name", ""), ("description", ""),
        ("parent_role_id", "fk"),
    ]),
    "APP_PERMISSION": (1560, 1460, False, [
        ("permission_id", "pk"), ("permission_code", ""),
        ("description", ""),
    ]),
}

# name -> (x, y, is_identifying, [attributes], [(entity, role, cardinality)])
RELATIONSHIPS: dict[str, tuple] = {
    "VOTE": (400, 120, True,
             ["granted", "candidate_last_index", "candidate_last_term",
              "voted_at"],
             [("TERM", "scopes", "1"), ("NODE", "voter", "M"),
              ("NODE", "candidate", "N")]),
    "REPLICATION_STATE": (880, 650, False,
                          ["next_index", "match_index", "last_contact"],
                          [("NODE", "leader", "M"), ("NODE", "follower", "N")]),
    "CLIENT_SESSION": (460, 760, True,
                       ["last_seq_seen", "last_result", "updated_at"],
                       [("NODE", "applies on", "M"),
                        ("CLIENT", "tracked for", "N")]),
    "USER_ROLE": (400, 1180, False,
                  ["granted_at"],
                  [("APP_USER", "holds", "M"), ("APP_ROLE", "held by", "N")]),
    "ROLE_PERMISSION": (1180, 1400, False,
                        [],
                        [("APP_ROLE", "grants", "M"),
                         ("APP_PERMISSION", "granted to", "N")]),
}

# (source, target, label, cardinality)
SIMPLE_RELATIONSHIPS = [
    ("TERM", "NODE", "elects", "1 : 0..1"),
    ("NODE", "LOG_ENTRY", "stores", "1 : N"),
    ("TERM", "LOG_ENTRY", "stamps", "1 : N"),
    ("NODE", "KV_STATE", "applies", "1 : N"),
    ("NODE", "SNAPSHOT", "captures", "1 : N"),
    ("NODE", "NODE_EVENT", "emits", "1 : N"),
    ("CLIENT", "CLIENT_REQUEST", "issues", "1 : N"),
    ("APP_USER", "CLIENT", "owns", "1 : N"),
    ("APP_USER", "AUTH_SESSION", "opens", "1 : N"),
    ("APP_USER", "KV_STATE", "owns key", "1 : N"),
    ("APP_ROLE", "APP_ROLE", "inherits from", "N : 1"),
]

ROW_H = 22
HEADER_H = 32
BOX_W = 260


def _box_height(attrs: list) -> int:
    return HEADER_H + ROW_H * len(attrs) + 8


# ----------------------------------------------------------------------
# draw.io
# ----------------------------------------------------------------------

def _attr_label(attr: str, kind: str) -> str:
    if kind == "pk":
        return f"<u>{attr}</u> <i>PK</i>"
    if kind == "pkfk":
        return f"<u>{attr}</u> <i>PK, FK</i>"
    if kind == "fk":
        return f"<i>{attr}  FK</i>"
    return attr


def build_drawio(path: str) -> None:
    cells: list[str] = []
    nid = iter(range(100, 100000))

    def esc(text: str) -> str:
        return html.escape(text, quote=True)

    ids: dict[str, str] = {}

    # Entities
    for name, (x, y, weak, attrs) in ENTITIES.items():
        cid = f"ent{next(nid)}"
        ids[name] = cid
        header = f"<b>{name}</b>"
        if weak:
            header += "<br/><font style='font-size:9px'>(weak entity)</font>"
        body = "<br/>".join(_attr_label(a, k) for a, k in attrs)
        label = f"{header}<hr size='1'/>{body}"
        # A weak entity gets the double border its notation calls for.
        style = (
            "rounded=0;whiteSpace=wrap;html=1;align=left;verticalAlign=top;"
            "spacingLeft=8;spacingTop=4;fontSize=11;"
            + ("shape=ext;double=1;fillColor=#FBE6D4;strokeColor=#B06B3A;"
               if weak else "fillColor=#D9E6F2;strokeColor=#3A6EA5;")
        )
        cells.append(
            f'<mxCell id="{cid}" value="{esc(label)}" style="{style}" '
            f'vertex="1" parent="1">'
            f'<mxGeometry x="{x}" y="{y}" width="{BOX_W}" '
            f'height="{_box_height(attrs)}" as="geometry"/></mxCell>')

    # Relationship diamonds
    for name, (x, y, identifying, attrs, legs) in RELATIONSHIPS.items():
        cid = f"rel{next(nid)}"
        ids[name] = cid
        label = f"<b>{name}</b>"
        if attrs:
            label += ("<br/><font style='font-size:9px'>"
                      + ", ".join(attrs) + "</font>")
        style = (
            "rhombus;whiteSpace=wrap;html=1;fontSize=11;"
            "fillColor=#E5F4E4;strokeColor=#3B7A2E;"
            + ("double=1;" if identifying else "")
        )
        width = 230 if attrs else 190
        cells.append(
            f'<mxCell id="{cid}" value="{esc(label)}" style="{style}" '
            f'vertex="1" parent="1">'
            f'<mxGeometry x="{x}" y="{y}" width="{width}" height="110" '
            f'as="geometry"/></mxCell>')

        for entity, role, card in legs:
            eid = f"edg{next(nid)}"
            cells.append(
                f'<mxCell id="{eid}" value="{esc(f"{role}  ({card})")}" '
                f'style="endArrow=none;html=1;fontSize=10;strokeWidth=2;" '
                f'edge="1" parent="1" source="{ids[entity]}" target="{cid}">'
                f'<mxGeometry relative="1" as="geometry"/></mxCell>')

    # Simple 1:N relationships, as labelled connectors
    for src, dst, label, card in SIMPLE_RELATIONSHIPS:
        eid = f"edg{next(nid)}"
        style = ("endArrow=block;endFill=1;html=1;fontSize=10;"
                 "strokeColor=#5F5E5A;")
        if src == dst:
            style += "edgeStyle=loopEdge;"
        cells.append(
            f'<mxCell id="{eid}" value="{esc(f"{label}  [{card}]")}" '
            f'style="{style}" edge="1" parent="1" '
            f'source="{ids[src]}" target="{ids[dst]}">'
            f'<mxGeometry relative="1" as="geometry"/></mxCell>')

    # Legend
    legend = (
        "<b>Legend</b><br/>"
        "Blue rectangle = entity<br/>"
        "Orange double rectangle = weak entity<br/>"
        "Green diamond = relationship that becomes a relation<br/>"
        "Double diamond = identifying relationship<br/>"
        "Labelled arrow = 1:N relationship with cardinality<br/>"
        "<u>underlined</u> = primary key, <i>italic</i> = foreign key"
    )
    cells.append(
        f'<mxCell id="legend" value="{esc(legend)}" '
        f'style="rounded=0;whiteSpace=wrap;html=1;align=left;'
        f'verticalAlign=top;spacingLeft=8;spacingTop=6;fontSize=11;'
        f'fillColor=#FFFFFF;strokeColor=#888780;" vertex="1" parent="1">'
        f'<mxGeometry x="40" y="40" width="300" height="140" '
        f'as="geometry"/></mxCell>')

    xml = (
        '<mxfile host="app.diagrams.net" type="device">\n'
        '  <diagram name="ER Model" id="raftkv-er">\n'
        '    <mxGraphModel dx="1400" dy="900" grid="1" gridSize="10" '
        'guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="1" '
        'pageScale="1" pageWidth="2000" pageHeight="1700" math="0" '
        'shadow="0">\n'
        '      <root>\n'
        '        <mxCell id="0"/>\n'
        '        <mxCell id="1" parent="0"/>\n'
        + "\n".join("        " + c for c in cells) + "\n"
        '      </root>\n'
        '    </mxGraphModel>\n'
        '  </diagram>\n'
        '</mxfile>\n'
    )

    with open(path, "w", encoding="utf-8") as fh:
        fh.write(xml)


# ----------------------------------------------------------------------
# PNG, from the same model
# ----------------------------------------------------------------------

def build_png(path: str) -> None:
    from graphviz import Digraph

    g = Digraph("ER", format="png")
    g.attr(rankdir="LR", bgcolor="white", fontname="Helvetica",
           splines="spline", nodesep="0.5", ranksep="1.4")
    g.attr("node", shape="plaintext", fontname="Helvetica")
    g.attr("edge", fontname="Helvetica", fontsize="9", color="#5F5E5A")

    def attr_row(attr: str, kind: str, bg: str) -> str:
        if kind == "pk":
            text = f"<U><B>{attr}</B></U>  PK"
        elif kind == "pkfk":
            text = f"<U><B>{attr}</B></U>  PK, FK"
        elif kind == "fk":
            text = f"<I>{attr}</I>  FK"
        else:
            text = attr
        return (f'<TR><TD BGCOLOR="{bg}" ALIGN="LEFT">'
                f'<FONT POINT-SIZE="9">{text}</FONT></TD></TR>')

    for name, (_, _, weak, attrs) in ENTITIES.items():
        hdr = "#FBE6D4" if weak else "#D9E6F2"
        border = "2" if weak else "1"
        title = name
        if weak:
            title += "<BR/><FONT POINT-SIZE='8'>(weak entity)</FONT>"
        rows = "".join(
            attr_row(a, k, "#F4F4F1" if i % 2 else "#FFFFFF")
            for i, (a, k) in enumerate(attrs))
        g.node(name, label=(
            f'<<TABLE BORDER="{border}" CELLBORDER="0" CELLSPACING="0" '
            f'CELLPADDING="4" COLOR="#888780">'
            f'<TR><TD BGCOLOR="{hdr}" ALIGN="CENTER">'
            f'<FONT POINT-SIZE="11"><B>{title}</B></FONT></TD></TR>'
            f'{rows}</TABLE>>'))

    for name, (_, _, identifying, attrs, legs) in RELATIONSHIPS.items():
        label = name
        if attrs:
            # A diamond auto-sizes to its label, so a single long line of
            # attributes stretches it off the canvas. Two per line keeps
            # every diamond inside the page.
            lines = [", ".join(attrs[i:i + 2]) for i in range(0, len(attrs), 2)]
            label += "\\n" + "\\n".join(lines)
        g.node(name, shape="diamond", style="filled", fillcolor="#E5F4E4",
               color="#3B7A2E", fontsize="10",
               penwidth="3" if identifying else "1.5",
               label=label, fontname="Helvetica")
        for entity, role, card in legs:
            g.edge(entity, name, label=f"{role} ({card})",
                   arrowhead="none", penwidth="1.6")

    for src, dst, label, card in SIMPLE_RELATIONSHIPS:
        g.edge(src, dst, label=f"{label} [{card}]", arrowhead="vee")

    g.render(os.path.splitext(path)[0], cleanup=True)


def main() -> None:
    drawio = os.path.join(OUT_DIR, "er_diagram.drawio")
    png = os.path.join(OUT_DIR, "er_diagram.png")

    build_drawio(drawio)
    print("wrote", drawio)

    build_png(png)
    print("wrote", png)


if __name__ == "__main__":
    main()
