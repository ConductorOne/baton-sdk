package main

import (
	cfg "github.com/conductorone/baton-appstoreconnect/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/config"
)

func main() {
	config.Generate("app-store-connect", cfg.Config)
}
