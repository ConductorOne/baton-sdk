package explorer

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os/exec"
	"runtime"

	"github.com/conductorone/baton-sdk/pkg/baton/storecache"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/gin-gonic/contrib/static"
	"github.com/gin-gonic/gin"
)

//go:embed frontend/*
var frontend embed.FS

type EmbededFS struct {
	http.FileSystem
}

func (efs EmbededFS) Exists(prefix string, path string) bool {
	_, err := efs.Open(path)
	return err == nil
}

func newEmbeddedFS(efs embed.FS) EmbededFS {
	httpfs, err := fs.Sub(efs, "frontend")
	if err != nil {
		panic(err)
	}
	return EmbededFS{
		FileSystem: http.FS(httpfs),
	}
}

type Controller struct {
	baton *BatonService
}

func NewController(ctx context.Context, store c1zstore.Store, syncID, resourceType string, devMode bool) (Controller, error) {
	return Controller{&BatonService{
		storeCache:   storecache.NewStoreCache(ctx, store),
		store:        store,
		syncID:       syncID,
		resourceType: resourceType,
		devMode:      devMode,
	}}, nil
}

func (ctrl *Controller) Run(addr string) error {
	return ctrl.router(addr).Run(addr)
}

func (ctrl *Controller) router(addr string) *gin.Engine {
	ctx := context.Background()
	if !ctrl.baton.devMode {
		gin.SetMode(gin.ReleaseMode)
	}
	router := gin.Default()
	api := router.Group("/api")

	efs := newEmbeddedFS(frontend)
	router.Use(static.Serve("/", efs))

	// todo: make this configurable
	if !ctrl.baton.devMode {
		err := openBrowser(ctx, browserURL(addr))
		if err != nil {
			log.Default().Print("error opening browser: ", err)
		}
	}

	// on reload it throws 404, so we need to redirect to index.html.
	router.NoRoute(func(ctx *gin.Context) {
		ctx.FileFromFS("index.html", efs)
	})

	{
		api.GET("/entitlements", ctrl.GetEntitlementsHandler)
		api.GET("/resources", ctrl.GetResourcesHandler)
		api.GET("/resourceTypes", ctrl.GetResourceTypesHandler)
		api.GET("/grants/:resourceType/:resourceId", ctrl.GetGrantsForResourceHandler)
		api.GET("/access/:resourceType/:resourceId", ctrl.GetAccessHandler)
		api.GET("/:resourceType/:resourceId", ctrl.GetResourceHandler)
		api.GET("/principals/:resourceType", ctrl.GetResourcesWithPrincipalCountHandler)
	}
	return router
}

// browserURL converts a listen address into a URL the local browser
// can open, mapping empty/wildcard hosts to localhost.
func browserURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://localhost" + addr
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "localhost"
	}
	return "http://" + net.JoinHostPort(host, port)
}

func openBrowser(ctx context.Context, url string) error {
	var err error
	switch runtime.GOOS {
	case "darwin":
		err = exec.CommandContext(ctx, "open", url).Start()
	case "linux":
		err = exec.CommandContext(ctx, "xdg-open", url).Start()
	case "windows":
		err = exec.CommandContext(ctx, "rundll32", "url.dll,FileProtocolHandler", url).Start()

	default:
		err = fmt.Errorf("platform not supported")
	}
	if err != nil {
		return fmt.Errorf("error opening browser: %w", err)
	}

	return nil
}
