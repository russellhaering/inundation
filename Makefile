GO=go run third_party.go
PKGNAME=github.com/russellhaering/inundation

build:
	$(GO) build $(PKGNAME)
	$(GO) build example.go
