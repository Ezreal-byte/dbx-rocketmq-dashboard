package main

import (
	"log"

	sdk "github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk"
)

// The sidecar speaks DBX's plugin protocol v1 over stdio. stdout is reserved
// for protocol frames, so every diagnostic must go to stderr - that is the
// default for the standard logger.
func main() {
	server := sdk.NewServer(sdk.Metadata{
		ID:           pluginID,
		Version:      pluginVersion,
		Capabilities: []string{"connections", "rocketmq-admin"},
	}, newPlugin())
	if err := server.Serve(); err != nil {
		log.Fatal(err)
	}
}
