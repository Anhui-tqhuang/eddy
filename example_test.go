package eddy_test

import (
	"context"
	"log"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Anhui-tqhuang/eddy"
)

// Service is one row of the services table.
type Service struct {
	ID    uuid.UUID `db:",default"`
	Image string
	Phase string `db:",status"` // written only by the controller
}

// Services is shared by the API and the controller.
var Services = eddy.NewTable[Service]("services")

func Example() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://localhost/app")
	if err != nil {
		log.Fatal(err)
	}
	db, err := eddy.Open(ctx, pool)
	if err != nil {
		log.Fatal(err)
	}

	// API: the write enqueues the reconcile itself.
	svc := Service{Image: "web:1"}
	if err := Services.Insert(ctx, db, &svc); err != nil {
		log.Fatal(err)
	}

	// Controller: run on any number of replicas; one leads.
	err = db.Run(ctx,
		eddy.Reconcile(Services, func(ctx context.Context, svc *Service) error {
			// Converge the outside world, then record the result.
			svc.Phase = "ready"
			return nil
		}),
		eddy.Teardown(Services, func(ctx context.Context, svc *Service) error {
			// svc is the row as it was when deleted.
			return nil
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
}
