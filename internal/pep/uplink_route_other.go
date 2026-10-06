//go:build !darwin

package pep

import "context"

func uplinkGateway(int, string) string                         { return "" }
func (c *Client) uplinkEvents(context.Context) <-chan struct{} { return nil }
