"""
Run once the Qwen host machine is up: python scripts/benchmark_qwen_throughput.py

Measures real AGGREGATE tokens/sec at increasing concurrency levels, not single-request
latency -- a batch classification job's wall-clock time depends on how much the serving stack
(vLLM/TGI/sglang all continuous-batch; a bare/naive setup mostly won't) can actually overlap
concurrent requests, which single-request timing doesn't reveal at all.

Requires QWEN_API_BASE_URL (and optionally QWEN_API_KEY / QWEN_MODEL_NAME) set in the
environment or .env -- see src/integrations/llm/qwen_llm.py.
"""
import concurrent.futures
import statistics
import sys
import time

sys.path.insert(0, ".")
from src.integrations.llm import qwen_llm  # noqa: E402

# A realistic-shaped prompt, similar in size to a real Stage 2 batch call from the two-stage
# classification prototype (system instructions + a candidate list + a handful of parts) -- a
# trivial "say hi" prompt would under-report real throughput since prompt processing time scales
# with input length too, not just generation.
_BENCH_SYSTEM = """You classify each auto-parts product into the single best-matching PCdb part
terminology from the candidate list given. Return strict JSON: {"parts": [{"id": <int>,
"terminology_id": <int or null>, "confidence": <0.0-1.0>, "reasoning": "<one sentence>"}, ...]}."""

_BENCH_CANDIDATES = "\n".join(f"{1000+i}: Sample Terminology Name {i}" for i in range(40))
_BENCH_USER = f"""Candidates:
{_BENCH_CANDIDATES}

Parts:
1: DORMAN | BRK CABLE EA.
2: MAHLE | PISTONS SET OF 8
3: ACDELCO | A/C COMPRESSOR
4: ACDELCO | KEY FOB
5: DORMAN | CLUTCH SLAVE EA."""

MAX_TOKENS = 800


def one_call():
    cli = qwen_llm.client()
    t0 = time.monotonic()
    parsed, err = qwen_llm.complete_json(cli, _BENCH_SYSTEM, _BENCH_USER, max_tokens=MAX_TOKENS)
    elapsed = time.monotonic() - t0
    return elapsed, err, parsed is not None


def run_at_concurrency(n: int, calls_per_worker: int = 2):
    total_calls = n * calls_per_worker
    print(f"\n--- concurrency={n} ({total_calls} total calls) ---", flush=True)
    t0 = time.monotonic()
    latencies = []
    errors = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=n) as pool:
        futures = [pool.submit(one_call) for _ in range(total_calls)]
        for f in concurrent.futures.as_completed(futures):
            elapsed, err, ok = f.result()
            latencies.append(elapsed)
            if err or not ok:
                errors += 1
                print(f"  call failed: {err}", flush=True)
    wall_time = time.monotonic() - t0

    print(f"  wall time: {wall_time:.1f}s for {total_calls} calls ({errors} errors)", flush=True)
    print(f"  avg latency/call: {statistics.mean(latencies):.1f}s  p90: {sorted(latencies)[int(len(latencies) * 0.9)]:.1f}s", flush=True)
    print(f"  throughput: {total_calls / wall_time:.2f} calls/sec", flush=True)
    return total_calls / wall_time


if __name__ == "__main__":
    print("=== Qwen throughput benchmark: single request first, then concurrency scaling ===", flush=True)
    for n in [1, 5, 10, 20, 50]:
        try:
            run_at_concurrency(n)
        except Exception as e:
            print(f"  concurrency={n} failed outright: {e!r} -- stopping here, that's your real ceiling", flush=True)
            break
    print("\n=== DONE -- the concurrency level where calls/sec stops increasing is your real capacity. ===", flush=True)
