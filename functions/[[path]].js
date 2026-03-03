export async function onRequest(context) {
  const { request, env } = context;
  const url = new URL(request.url);

  if (!url.pathname.endsWith(".parquet")) {
    return env.ASSETS.fetch(request);
  }

  const response = await env.ASSETS.fetch(request);

  if (!request.headers.has("Range")) {
    const headers = new Headers(response.headers);
    headers.set("Accept-Ranges", "bytes");
    return new Response(response.body, { status: response.status, headers });
  }

  const body = await response.arrayBuffer();
  const total = body.byteLength;
  const rangeHeader = request.headers.get("Range");
  const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);

  if (!match) {
    return new Response(body, {
      status: 200,
      headers: {
        "Content-Type": response.headers.get("Content-Type") || "application/octet-stream",
        "Content-Length": total,
        "Accept-Ranges": "bytes",
      },
    });
  }

  const start = parseInt(match[1], 10);
  const end = match[2] ? parseInt(match[2], 10) : total - 1;
  const slice = body.slice(start, end + 1);

  return new Response(slice, {
    status: 206,
    headers: {
      "Content-Type": response.headers.get("Content-Type") || "application/octet-stream",
      "Content-Range": `bytes ${start}-${end}/${total}`,
      "Content-Length": slice.byteLength,
      "Accept-Ranges": "bytes",
    },
  });
}
