curl -o profiles/cpu.pb.gz       "http://localhost:6060/debug/pprof/profile?seconds=30"

curl -o profiles/heap.pb.gz      "http://localhost:6060/debug/pprof/heap"
curl -o profiles/goroutine.pb.gz "http://localhost:6060/debug/pprof/goroutine"
curl -o profiles/block.pb.gz     "http://localhost:6060/debug/pprof/block"
curl -o profiles/mutex.pb.gz     "http://localhost:6060/debug/pprof/mutex"

go tool pprof -top .\profiles\cpu.pb.gz
go tool pprof -top .\profiles\heap.pb.gz
go tool pprof -top .\profiles\goroutine.pb.gz
go tool pprof -top .\profiles\block.pb.gz
go tool pprof -top .\profiles\mutex.pb.gz