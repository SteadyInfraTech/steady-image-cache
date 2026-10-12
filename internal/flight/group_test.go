package flight
import("sync";"sync/atomic";"testing";"time")
func TestCoalesces(t *testing.T){var g Group; var n atomic.Int32; var wg sync.WaitGroup; for i:=0;i<20;i++{wg.Add(1);go func(){defer wg.Done(); if !g.Do("x",func()bool{n.Add(1);time.Sleep(20*time.Millisecond);return true}){t.Error("false")}}()};wg.Wait();if n.Load()!=1{t.Fatalf("runs=%d",n.Load())}}
