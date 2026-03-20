import type { Metadata } from 'next';
import './globals.css';
import PlaylistPanel from '@/components/PlaylistPanel';

export const metadata: Metadata = {
  title: 'Nexus — Deep Research Engine',
  description: 'AI-powered deep research with multi-pass search, gap analysis, and comprehensive synthesis',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className="min-h-screen bg-dark-950 text-dark-100 antialiased pb-14">
        {children}
        <PlaylistPanel />
      </body>
    </html>
  );
}
