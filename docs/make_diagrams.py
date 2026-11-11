"""Regenerate the ER diagram and the relational schema diagram.

Both are produced from one declaration of the schema below, so they can
never drift apart. Run after any change to sql/01_schema.sql:

    python docs/make_diagrams.py
"""

from __future__ import annotations

import os
import subprocess

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

# (relation, [(attribute, kind)]) where kind is pk | pkfk | fk | ''
RELATIONS = [
    ("NODE", [
        ("node_id", "pk"), ("host_address", ""), ("port", ""), ("role", ""),
        ("current_term", "fk"), ("voted_for", "fk"), ("commit_index", ""),
        ("last_applied", ""), ("is_alive", ""),
    ]),
    ("TERM", [
        ("term_number", "pk"), ("leader_id", "fk"), ("started_at", ""),
        ("ended_at", ""),
    ]),
    ("VOTE", [
        ("term_number", "pkfk"), ("voter_id", "pkfk"), ("candidate_id", "fk"),
        ("granted", ""), ("candidate_last_index", ""),
        ("candidate_last_term", ""), ("voted_at", ""),
    ]),
    ("LOG_ENTRY", [
        ("node_id", "pkfk"), ("log_index", "pk"), ("term_number", "fk"),
        ("command_type", ""), ("key_name", ""), ("value_text", ""),
        ("client_id", ""), ("seq_no", ""), ("committed", ""),
    ]),
    ("KV_STATE", [
        ("node_id", "pkfk"), ("key_name", "pk"), ("value_text", ""),
        ("last_applied_index", ""), ("owner_user_id", "fk"), ("updated_at", ""),
    ]),
    ("SNAPSHOT", [
        ("snapshot_id", "pk"), ("node_id", "fk"), ("last_included_index", ""),
        ("last_included_term", ""), ("state_blob", ""),
    ]),
    ("REPLICATION_STATE", [
        ("leader_id", "pkfk"), ("follower_id", "pkfk"), ("next_index", ""),
        ("match_index", ""), ("last_contact", ""),
    ]),
    ("CLIENT", [
        ("client_id", "pk"), ("user_id", "fk"), ("opened_at", ""),
    ]),
    ("CLIENT_SESSION", [
        ("node_id", "pkfk"), ("client_id", "pkfk"), ("last_seq_seen", ""),
        ("last_result", ""), ("updated_at", ""),
    ]),
    ("CLIENT_REQUEST", [
        ("request_id", "pk"), ("client_id", "fk"), ("seq_no", ""),
        ("command_type", ""), ("assigned_index", "fk"), ("status", ""),
    ]),
    ("NODE_EVENT", [
        ("event_id", "pk"), ("node_id", "fk"), ("event_type", ""),
        ("old_value", ""), ("new_value", ""), ("description", ""),
        ("occurred_at", ""),
    ]),
    ("APP_USER", [
        ("user_id", "pk"), ("username", ""), ("email", ""),
        ("password_hash", ""), ("is_active", ""), ("created_at", ""),
    ]),
    ("APP_ROLE", [
        ("role_id", "pk"), ("role_name", ""), ("description", ""),
        ("parent_role_id", "fk"),
    ]),
    ("APP_PERMISSION", [
        ("permission_id", "pk"), ("permission_code", ""), ("description", ""),
    ]),
    ("USER_ROLE", [
        ("user_id", "pkfk"), ("role_id", "pkfk"), ("granted_at", ""),
    ]),
    ("ROLE_PERMISSION", [
        ("role_id", "pkfk"), ("permission_id", "pkfk"),
    ]),
    ("AUTH_SESSION", [
        ("token_hash", "pk"), ("user_id", "fk"), ("issued_at", ""),
        ("expires_at", ""), ("revoked", ""),
    ]),
]

# (source relation, source attribute, target relation, target attribute)
FOREIGN_KEYS = [
    ("NODE", "voted_for", "NODE", "node_id"),
    ("NODE", "current_term", "TERM", "term_number"),
    ("TERM", "leader_id", "NODE", "node_id"),
    ("VOTE", "term_number", "TERM", "term_number"),
    ("VOTE", "voter_id", "NODE", "node_id"),
    ("VOTE", "candidate_id", "NODE", "node_id"),
    ("LOG_ENTRY", "node_id", "NODE", "node_id"),
    ("LOG_ENTRY", "term_number", "TERM", "term_number"),
    ("KV_STATE", "node_id", "NODE", "node_id"),
    ("KV_STATE", "owner_user_id", "APP_USER", "user_id"),
    ("SNAPSHOT", "node_id", "NODE", "node_id"),
    ("REPLICATION_STATE", "leader_id", "NODE", "node_id"),
    ("REPLICATION_STATE", "follower_id", "NODE", "node_id"),
    ("CLIENT", "user_id", "APP_USER", "user_id"),
    ("CLIENT_SESSION", "node_id", "NODE", "node_id"),
    ("CLIENT_SESSION", "client_id", "CLIENT", "client_id"),
    ("CLIENT_REQUEST", "client_id", "CLIENT", "client_id"),
    ("NODE_EVENT", "node_id", "NODE", "node_id"),
    ("APP_ROLE", "parent_role_id", "APP_ROLE", "role_id"),
    ("USER_ROLE", "user_id", "APP_USER", "user_id"),
    ("USER_ROLE", "role_id", "APP_ROLE", "role_id"),
    ("ROLE_PERMISSION", "role_id", "APP_ROLE", "role_id"),
    ("ROLE_PERMISSION", "permission_id", "APP_PERMISSION", "permission_id"),
    ("AUTH_SESSION", "user_id", "APP_USER", "user_id"),
]

# Crow's-foot relationships for the ER diagram.
ER_RELATIONSHIPS = [
    ("NODE", "LOG_ENTRY", "stores", "1:N"),
    ("NODE", "KV_STATE", "applies", "1:N"),
    ("NODE", "SNAPSHOT", "captures", "1:N"),
    ("NODE", "VOTE", "casts", "1:N"),
    ("NODE", "REPLICATION_STATE", "tracks", "1:N"),
    ("NODE", "CLIENT_SESSION", "maintains", "1:N"),
    ("NODE", "NODE_EVENT", "emits", "1:N"),
    ("TERM", "VOTE", "scopes", "1:N"),
    ("TERM", "LOG_ENTRY", "stamps", "1:N"),
    ("TERM", "NODE", "elects", "1:1"),
    ("CLIENT", "CLIENT_SESSION", "tracked by", "1:N"),
    ("CLIENT", "CLIENT_REQUEST", "issues", "1:N"),
    ("APP_USER", "USER_ROLE", "holds", "1:N"),
    ("APP_ROLE", "USER_ROLE", "assigned in", "1:N"),
    ("APP_ROLE", "ROLE_PERMISSION", "grants", "1:N"),
    ("APP_PERMISSION", "ROLE_PERMISSION", "granted in", "1:N"),
    ("APP_ROLE", "APP_ROLE", "inherits", "1:N"),
    ("APP_USER", "AUTH_SESSION", "opens", "1:N"),
    ("APP_USER", "CLIENT", "owns", "1:N"),
]

WEAK_ENTITIES = {"LOG_ENTRY", "KV_STATE", "CLIENT_SESSION",
                 "REPLICATION_STATE", "VOTE", "USER_ROLE", "ROLE_PERMISSION"}


# ---------------------------------------------------------------------
# ER diagram
# ---------------------------------------------------------------------

def build_er_diagram(path: str) -> None:
    from graphviz import Digraph

    g = Digraph("ER", format="png")
    g.attr(rankdir="LR", bgcolor="white", fontname="Helvetica",
           splines="spline", nodesep="0.45", ranksep="1.3", concentrate="false")
    g.attr("node", shape="plaintext", fontname="Helvetica")
    g.attr("edge", fontname="Helvetica", fontsize="9", color="#5F5E5A")

    strong_hdr, weak_hdr, row, alt = "#D9E6F2", "#FBE6D4", "#FFFFFF", "#F4F4F1"

    for name, attrs in RELATIONS:
        hdr = weak_hdr if name in WEAK_ENTITIES else strong_hdr
        label_name = f"{name}<BR/><FONT POINT-SIZE='8'>(weak entity)</FONT>" \
            if name in WEAK_ENTITIES else name
        rows = ""
        for i, (attr, kind) in enumerate(attrs):
            bg = alt if i % 2 else row
            if kind == "pk":
                text = f"<U><B>{attr}</B></U>  PK"
            elif kind == "pkfk":
                text = f"<U><B>{attr}</B></U>  PK, FK"
            elif kind == "fk":
                text = f"<I>{attr}</I>  FK"
            else:
                text = attr
            rows += (f'<TR><TD BGCOLOR="{bg}" ALIGN="LEFT">'
                     f'<FONT POINT-SIZE="9">{text}</FONT></TD></TR>')
        g.node(name, label=(
            f'<<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0" '
            f'CELLPADDING="4" COLOR="#888780">'
            f'<TR><TD BGCOLOR="{hdr}" ALIGN="CENTER">'
            f'<FONT POINT-SIZE="11"><B>{label_name}</B></FONT></TD></TR>'
            f'{rows}</TABLE>>'))

    for src, dst, label, card in ER_RELATIONSHIPS:
        head = "crow" if card == "1:N" else "none"
        g.edge(src, dst, label=label, arrowtail="none", arrowhead=head,
               dir="both")

    g.render(os.path.splitext(path)[0], cleanup=True)


# ---------------------------------------------------------------------
# Relational schema diagram, in the Elmasri & Navathe style
# ---------------------------------------------------------------------

def build_schema_diagram(svg_path: str) -> None:
    FS, TITLE_FS, CH, PAD = 11.5, 13, 28, 20
    X0, Y0, TITLE_GAP, GAP = 55, 36, 20, 62

    def cw(label: str) -> int:
        return max(54, int(len(label) * 6.9) + PAD)

    geom: dict[str, dict] = {}
    y = Y0
    for name, attrs in RELATIONS:
        x = X0
        cells = {}
        for attr, kind in attrs:
            w = cw(attr)
            cells[attr] = {"x": x, "w": w, "kind": kind}
            x += w
        geom[name] = {"title_y": y, "box_y": y + TITLE_GAP,
                      "cells": cells, "right": x}
        y += TITLE_GAP + CH + GAP

    diagram_h = y - GAP + 30
    max_right = max(g["right"] for g in geom.values())

    def span(fk):
        return abs(geom[fk[0]]["box_y"] - geom[fk[2]]["box_y"])

    order = sorted(range(len(FOREIGN_KEYS)), key=lambda i: span(FOREIGN_KEYS[i]))
    channel = {idx: max_right + 22 + rank * 17 for rank, idx in enumerate(order)}

    lane_count: dict[str, int] = {}
    approach: dict[tuple, int] = {}
    paths = []

    for idx, (sr, sa, tr, ta) in enumerate(FOREIGN_KEYS):
        sg, tg = geom[sr], geom[tr]
        sc, tc = sg["cells"][sa], tg["cells"][ta]
        sx = sc["x"] + sc["w"] / 2
        sy = sg["box_y"] + CH
        ty = tg["box_y"] + CH

        k = lane_count.get(sr, 0)
        lane_count[sr] = k + 1
        lane_y = sy + 9 + k * 7

        j = approach.get((tr, ta), 0)
        approach[(tr, ta)] = j + 1
        app_y = ty + 11 + j * 7

        n_in = sum(1 for f in FOREIGN_KEYS if f[2] == tr and f[3] == ta)
        if n_in > 1:
            usable = tc["w"] - 14
            tx = tc["x"] + 7 + usable * (j + 0.5) / n_in
        else:
            tx = tc["x"] + tc["w"] / 2

        chx = channel[idx]
        paths.append(
            f"M {sx:.1f} {sy} L {sx:.1f} {lane_y:.1f} L {chx} {lane_y:.1f} "
            f"L {chx} {app_y:.1f} L {tx:.1f} {app_y:.1f} L {tx:.1f} {ty + 5}")

    W = max_right + 22 + len(FOREIGN_KEYS) * 17 + 28
    H = diagram_h + 95

    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
        f'viewBox="0 0 {W} {H}" font-family="Helvetica, Arial, sans-serif">',
        f'<rect width="{W}" height="{H}" fill="#ffffff"/>',
        '<defs><marker id="ah" viewBox="0 0 10 10" refX="9" refY="5" '
        'markerWidth="5" markerHeight="5" orient="auto-start-reverse">'
        '<path d="M0 0 L10 5 L0 10 z" fill="#1A1A1A"/></marker></defs>',
    ]

    for name, attrs in RELATIONS:
        g = geom[name]
        out.append(f'<text x="{X0}" y="{g["title_y"] + 12}" '
                   f'font-size="{TITLE_FS}" font-weight="bold" '
                   f'fill="#1A1A1A">{name}</text>')
        for attr, kind in attrs:
            c = g["cells"][attr]
            fill = ("#FFF6DC" if kind in ("pk", "pkfk")
                    else "#EDF4E4" if kind == "fk" else "#F2F2EF")
            out.append(
                f'<rect x="{c["x"]}" y="{g["box_y"]}" width="{c["w"]}" '
                f'height="{CH}" fill="{fill}" stroke="#3A3A3A" stroke-width="0.9"/>')
            deco = ' text-decoration="underline"' if kind in ("pk", "pkfk") else ""
            style = ' font-style="italic"' if kind == "fk" else ""
            out.append(
                f'<text x="{c["x"] + c["w"] / 2:.1f}" '
                f'y="{g["box_y"] + CH / 2 + 4:.1f}" font-size="{FS}" '
                f'text-anchor="middle" fill="#1A1A1A"{deco}{style}>{attr}</text>')

    for d in paths:
        out.append(f'<path d="{d}" fill="none" stroke="#1A1A1A" '
                   f'stroke-width="1.1" marker-end="url(#ah)"/>')

    ly = diagram_h + 16
    out.append(f'<text x="{X0}" y="{ly}" font-size="12" font-weight="bold" '
               f'fill="#1A1A1A">Legend</text>')
    cx = X0
    for fill, text in (("#FFF6DC", "Primary key (underlined)"),
                       ("#EDF4E4", "Foreign key (italic)"),
                       ("#F2F2EF", "Non-key attribute")):
        out.append(f'<rect x="{cx}" y="{ly + 11}" width="24" height="16" '
                   f'fill="{fill}" stroke="#3A3A3A" stroke-width="0.9"/>')
        out.append(f'<text x="{cx + 30}" y="{ly + 23}" font-size="11" '
                   f'fill="#1A1A1A">{text}</text>')
        cx += 30 + len(text) * 6.2 + 26
    out.append(f'<path d="M {cx} {ly + 27} L {cx} {ly + 13}" fill="none" '
               f'stroke="#1A1A1A" stroke-width="1.1" marker-end="url(#ah)"/>')
    out.append(f'<text x="{cx + 10}" y="{ly + 23}" font-size="11" '
               f'fill="#1A1A1A">Foreign key references primary key</text>')
    out.append("</svg>")

    with open(svg_path, "w") as fh:
        fh.write("\n".join(out))


def main() -> None:
    """Generate the relational schema diagram.

    The ER diagram is NOT generated here any more. It lives in
    make_er_diagram.py, which models the five many-to-many and ternary
    associations as relationships rather than as pre-resolved entity
    boxes, and emits a draw.io file alongside the PNG. The ER builder
    below is kept only because the RELATIONS declaration it shares is
    still the source for the schema diagram.
    """
    schema_svg = os.path.join(OUT_DIR, "relational_schema.svg")
    schema_png = os.path.join(OUT_DIR, "relational_schema.png")

    build_schema_diagram(schema_svg)
    # LibreOffice renders the SVG reliably without a cairo dependency.
    subprocess.run(
        ["soffice", "--headless", "--convert-to", "png",
         "--outdir", OUT_DIR, schema_svg],
        check=False, capture_output=True)
    print("wrote", schema_png)


if __name__ == "__main__":
    main()
