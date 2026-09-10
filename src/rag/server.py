from http.server import BaseHTTPRequestHandler, HTTPServer
import json

from src.rag.pipeline import run_rag_pipeline


class RAGHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        if self.path != "/rag":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)

        try:
            payload = json.loads(body)
            query = payload.get(
                "query",
                "I need a lightweight laptop for traveling"
            )

            result = run_rag_pipeline(query)

            response = {
                "query": result["query"],
                "answer": result["answer"],
                "citations": result["citations"],
                "top_product": (
                    result["reranked_results"][0]["product_id"]
                    if result["reranked_results"]
                    else None
                ),
            }

            data = json.dumps(
                response,
                ensure_ascii=False
            ).encode("utf-8")

            self.send_response(200)
            self.send_header(
                "Content-Type",
                "application/json; charset=utf-8"
            )
            self.send_header(
                "Content-Length",
                str(len(data))
            )
            self.end_headers()
            self.wfile.write(data)

        except Exception as exc:
            data = json.dumps(
                {"error": str(exc)},
                ensure_ascii=False
            ).encode("utf-8")

            self.send_response(500)
            self.send_header(
                "Content-Type",
                "application/json; charset=utf-8"
            )
            self.send_header(
                "Content-Length",
                str(len(data))
            )
            self.end_headers()
            self.wfile.write(data)

    def log_message(self, format, *args):
        print(format % args)


if __name__ == "__main__":
    print("LiveCart RAG service listening on http://0.0.0.0:8000")
    HTTPServer(("0.0.0.0", 8000), RAGHandler).serve_forever()
