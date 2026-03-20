'use client';

import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import remarkMath from 'remark-math';

interface AnswerRendererProps {
  content: string;
}

export default function AnswerRenderer({ content }: AnswerRendererProps) {
  // Strip follow-up questions from display
  const cleanContent = content.replace(
    /<<<FOLLOW_UPS>>>.*?<<<END_FOLLOW_UPS>>>/s,
    ''
  );

  return (
    <div className="markdown-content">
      <ReactMarkdown remarkPlugins={[remarkGfm, remarkMath]}>
        {cleanContent}
      </ReactMarkdown>
    </div>
  );
}
