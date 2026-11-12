package eddy_test

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Anhui-tqhuang/eddy"
)

// Service is one row of the services table.
type Service struct {
	ID    uuid.UUID `db:"id,default"`
	Image string    `db:"image"`
	Phase string    `db:"phase,status"` // written only by the controller
}

// Services is shared by the API and the controller.
var Services = eddy.NewTable[Service, uuid.UUID]("services")

func Example() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://localhost/app")
	if err != nil {
		log.Fatal(err)
	}

	// Controller process: reconcile services, one replica at a time.
	ctl, err := eddy.New(ctx, pool,
		eddy.Reconcile(Services, func(ctx context.Context, id uuid.UUID, svc *Service) (eddy.Result, error) {
			if svc == nil {
				return eddy.Done, nil // row deleted: tear down what id stood for
			}
			// Converge the outside world, then record the result.
			svc.Phase = "ready"
			return eddy.Done, nil
		}),
		eddy.ResyncAll(Services, 10*time.Minute),
	)
	if err != nil {
		log.Fatal(err)
	}
	go func() { _ = ctl.Run(ctx) }()

	// API process: the write enqueues the reconcile itself.
	db, err := eddy.Open(ctx, pool)
	if err != nil {
		log.Fatal(err)
	}
	svc := Service{Image: "web:1"}
	if err := Services.Insert(ctx, db, &svc); err != nil {
		log.Fatal(err)
	}
}
