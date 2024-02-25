package listings

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/arrrden/hykafka"
)

type Listings struct{}

type NewListingReq struct {
	Name string `json:"name"`
}
type UpdatedListingReq struct {
	Name string `json:"name"`
}

func (g Listings) NewListing(rq NewListingReq) error {
	log.Println("new listing: ", rq.Name)
	return nil
}

func (g Listings) UpdatedListing(rq UpdatedListingReq) error {
	log.Println("updated listing: ", rq.Name)
	return nil
}

type ListingsHandler struct {
	srv *Listings
}

func NewListingsHandler(g *Listings) *ListingsHandler {
	return &ListingsHandler{srv: g}
}

func (gh *ListingsHandler) NewListing(ctx context.Context, data []byte) error {
	req, err := kafka.DecodeMessage[NewListingReq](data, NewListingReq{})
	if err != nil {
		return fmt.Errorf("NewListing error - %w", err)
	}
	return gh.srv.NewListing(*req)
}

func (gh *ListingsHandler) UpdatedListing(ctx context.Context, data []byte) error {
	req, err := kafka.DecodeMessage[UpdatedListingReq](data, UpdatedListingReq{})
	if err != nil {
		return fmt.Errorf("UpdatedListing error - %w", err)
	}
	return gh.srv.UpdatedListing(*req)
}

func (gh *ListingsHandler) DefaultHandler(ctx context.Context, data []byte) error {
	log.Println("Message processed by default handler: ", "\nmsg payload:", string(data))
	return nil
}
