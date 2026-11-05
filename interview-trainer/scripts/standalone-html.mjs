// The two HTML shapes of the single-file build, kept apart from the build script so they can be unit tested.
//
//  - fragment: for hosts that wrap a snippet in their own page (a claude.ai artifact). No doctype or <html>, because
//    the host supplies them; a leading <meta charset> and viewport are harmless there, and they make the bytes correct
//    when a host serves the snippet as-is.
//  - document: a complete page for opening the file directly (file://) or serving it as a plain static file. Without a
//    doctype a browser uses quirks mode, without a charset it reads UTF-8 as Latin-1 ("Â·" instead of "·"), and without
//    a viewport a phone lays the page out 980px wide.

const HEAD_META = '<meta charset="utf-8">\n<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">';
const TITLE = 'Interview Trainer';

// An inline <script> ends at the first "</script" (and "<!--" can start a script-data escape): neutralise both inside the bundle.
// `\x3C` is valid inside strings, template literals and (unicode-mode) regular expressions alike.
export const escapeInlineScript = (js) => js.replace(/<\/script/gi, '<\\/script').replace(/<!--/g, '\\x3C!--');

const body = (style, script) => `<style>
${style}
</style>
<div id="root"></div>
<script type="module">
${escapeInlineScript(script)}
</script>
`;

export const buildFragment = ({ style, script }) => `${HEAD_META}\n<title>${TITLE}</title>\n${body(style, script)}`;

export const buildDocument = ({ style, script }) => `<!doctype html>
<html lang="en">
<head>
${HEAD_META}
<meta name="theme-color" content="#F2F4F6" media="(prefers-color-scheme: light)">
<meta name="theme-color" content="#0D1721" media="(prefers-color-scheme: dark)">
<title>${TITLE}</title>
</head>
<body>
${body(style, script)}</body>
</html>
`;
