wrk.method = "PUT"
wrk.path = "/kv/bench"
wrk.body = "benchmark-value"
wrk.headers["Content-Type"] = "text/plain"
