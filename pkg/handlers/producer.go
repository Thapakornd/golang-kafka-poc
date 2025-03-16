package handlers

import "github.com/gofiber/fiber/v2"

type Producer struct{}

func NewProducer(app *fiber.App) *Producer {
	return &Producer{}
}

func (h *Producer) Produce(ctx *fiber.Ctx) error {
	return ctx.Status(fiber.StatusOK).JSON("success produce!")
}
