import "./globals.css";

export const metadata = {
  title: "RaftKV - Distributed Key-Value Store",
  description:
    "Dashboard for a fault-tolerant distributed key-value store built on the Raft consensus algorithm",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
