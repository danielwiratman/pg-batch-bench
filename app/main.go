package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

var (
	updateCount = 1_000_000
	runs        = 3
)

var states = []string{"QUEUED", "PROCESSING", "DONE", "FAILED"}

type UpdateJob struct {
	ID    int64
	State string
}

func main() {
	if len(os.Args) > 1 {
		n, err := strconv.ParseInt(os.Args[1], 10, 64)
		if err != nil || n <= 0 {
			log.Fatalf("invalid number: %v", os.Args[1])
		}
		updateCount = int(n)
	}

	_ = godotenv.Load()
	connString := os.Getenv("PG_CONN")
	if connString == "" {
		log.Fatal("PG_CONN not set")
	}

	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	fmt.Printf("Benchmarking %d updates, %d runs\n\n", updateCount, runs)

	run("1) Individual updates (multi-worker, 1 tx per row)", runs,
		func() float64 { return benchIndividual(pool) })

	run("2) Individual batched tx (10k per tx, multi-worker)", runs,
		func() float64 { return benchIndividualBatchTx(pool) })

	run("3) Batch via VALUES (chunked)", runs,
		func() float64 { return benchValues(pool) })

	run("4) Batch via TEMP TABLE (chunked insert, single update)", runs,
		func() float64 { return benchTemp(pool) })

	run("5) Batch via COPY protocol (Temp table + Join)", runs,
		func() float64 { return benchCopy(pool) })
}

func run(name string, count int, fn func() float64) {
	fmt.Println(name)
	var sum float64
	for i := 1; i <= count; i++ {
		t := fn()
		sum += t
		fmt.Printf("  run %d: %.3f s\n", i, t)
	}
	fmt.Printf("  AVG: %.3f s\n\n", sum/float64(count))
}

func makeJobs(n int) []UpdateJob {
	j := make([]UpdateJob, n)
	for i := range n {
		j[i] = UpdateJob{
			ID:    int64(i + 1),
			State: states[rand.Intn(len(states))],
		}
	}
	return j
}

func benchIndividual(pool *pgxpool.Pool) float64 {
	ctx := context.Background()
	jobs := makeJobs(updateCount)
	workers := runtime.NumCPU()

	ch := make(chan UpdateJob, 2048)
	var wg sync.WaitGroup
	wg.Add(workers)

	start := time.Now()

	for range workers {
		go func() {
			defer wg.Done()
			for job := range ch {
				_, err := pool.Exec(ctx,
					"UPDATE state_machine SET state=$1, updated_at=now() WHERE id=$2",
					job.State, job.ID)
				if err != nil {
					log.Fatal(err)
				}
			}
		}()
	}

	for _, job := range jobs {
		ch <- job
	}
	close(ch)

	wg.Wait()
	return time.Since(start).Seconds()
}

func benchIndividualBatchTx(pool *pgxpool.Pool) float64 {
	ctx := context.Background()
	jobs := makeJobs(updateCount)

	const chunk = 10_000
	var wg sync.WaitGroup

	start := time.Now()

	for i := 0; i < len(jobs); i += chunk {
		end := min(i+chunk, len(jobs))
		part := jobs[i:end]

		wg.Add(1)
		go func(batch []UpdateJob) {
			defer wg.Done()
			tx, err := pool.Begin(ctx)
			if err != nil {
				log.Fatal(err)
			}
			for _, job := range batch {
				_, err := tx.Exec(ctx,
					"UPDATE state_machine SET state=$1, updated_at=now() WHERE id=$2",
					job.State, job.ID)
				if err != nil {
					_ = tx.Rollback(ctx)
					log.Fatal(err)
				}
			}
			if err := tx.Commit(ctx); err != nil {
				log.Fatal(err)
			}
		}(part)
	}

	wg.Wait()
	return time.Since(start).Seconds()
}

func benchValues(pool *pgxpool.Pool) float64 {
	ctx := context.Background()
	jobs := makeJobs(updateCount)

	const chunk = 5_000
	start := time.Now()

	for i := 0; i < len(jobs); i += chunk {
		end := min(i+chunk, len(jobs))
		part := jobs[i:end]

		values := ""
		args := []any{}
		pos := 1

		for idx, job := range part {
			if idx > 0 {
				values += ","
			}
			values += fmt.Sprintf("($%d::bigint,$%d::text)", pos, pos+1)
			args = append(args, job.ID, job.State)
			pos += 2
		}

		query := fmt.Sprintf(`
			WITH updates(id,new_state) AS (VALUES %s)
			UPDATE state_machine sm
			SET state=updates.new_state, updated_at=now()
			FROM updates
			WHERE sm.id=updates.id;
		`, values)

		_, err := pool.Exec(ctx, query, args...)
		if err != nil {
			log.Fatal(err)
		}
	}

	return time.Since(start).Seconds()
}

func benchTemp(pool *pgxpool.Pool) float64 {
	ctx := context.Background()
	jobs := makeJobs(updateCount)

	const chunk = 10_000
	start := time.Now()

	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}

	_, err = tx.Exec(ctx, "CREATE TEMP TABLE tmp_updates(id bigint, new_state text) ON COMMIT DROP")
	if err != nil {
		_ = tx.Rollback(ctx)
		log.Fatal(err)
	}

	for i := 0; i < len(jobs); i += chunk {
		end := min(i+chunk, len(jobs))
		part := jobs[i:end]

		values := ""
		args := []any{}
		pos := 1

		for idx, job := range part {
			if idx > 0 {
				values += ","
			}
			values += fmt.Sprintf("($%d,$%d)", pos, pos+1)
			args = append(args, job.ID, job.State)
			pos += 2
		}

		_, err = tx.Exec(ctx,
			fmt.Sprintf("INSERT INTO tmp_updates(id,new_state) VALUES %s", values),
			args...)
		if err != nil {
			_ = tx.Rollback(ctx)
			log.Fatal(err)
		}
	}

	_, err = tx.Exec(ctx,
		"UPDATE state_machine sm SET state=t.new_state, updated_at=now() FROM tmp_updates t WHERE sm.id=t.id")
	if err != nil {
		_ = tx.Rollback(ctx)
		log.Fatal(err)
	}

	if err := tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}

	return time.Since(start).Seconds()
}

func benchCopy(pool *pgxpool.Pool) float64 {
	ctx := context.Background()
	jobs := makeJobs(updateCount)
	start := time.Now()

	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback(ctx)

	// 1. Create a temp table matching the structure
	// ON COMMIT DROP ensures it vanishes at the end of the transaction
	_, err = tx.Exec(ctx, "CREATE TEMP TABLE tmp_copy_updates(id bigint, state text) ON COMMIT DROP")
	if err != nil {
		log.Fatal(err)
	}

	// 2. Use CopyFrom to stream data efficiently
	// We convert our slice of structs into a format pgx can consume
	inputRows := [][]any{}
	for _, j := range jobs {
		inputRows = append(inputRows, []any{j.ID, j.State})
	}

	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"tmp_copy_updates"},
		[]string{"id", "state"},
		pgx.CopyFromRows(inputRows),
	)
	if err != nil {
		log.Fatal(err)
	}

	// 3. Perform the bulk update with a join
	// This usually triggers a Hash Join or Merge Join, which is extremely fast
	_, err = tx.Exec(ctx, `
		UPDATE state_machine sm
		SET state = t.state, updated_at = now()
		FROM tmp_copy_updates t
		WHERE sm.id = t.id
	`)
	if err != nil {
		log.Fatal(err)
	}

	if err := tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}

	return time.Since(start).Seconds()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
