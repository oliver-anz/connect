package protobuf

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"buf.build/gen/go/bufbuild/reflect/connectrpc/go/buf/reflect/v1beta1/reflectv1beta1connect"
	connectrpc "connectrpc.com/connect"
	"github.com/bufbuild/prototransform"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type MultiModuleWatcher struct {
	bsrClients map[string]*prototransform.SchemaWatcher
}

var _ prototransform.Resolver = &MultiModuleWatcher{}

func newMultiModuleWatcher(bsrUrl string, bsrApiKey string, modules []string, versions []string) (*MultiModuleWatcher, error) {
	if len(modules) != len(versions) {
		return nil, fmt.Errorf("could not create MultiModuleWatcher as modules and versions were not the same length")
	}

	multiModuleWatcher := &MultiModuleWatcher{}

	// Initialise one client for each module
	multiModuleWatcher.bsrClients = make(map[string]*prototransform.SchemaWatcher)
	for i, _ := range modules {
		watcher, err := newSchemaWatcher(context.Background(), bsrUrl, bsrApiKey, modules[i], versions[i])
		if err != nil {
			return nil, err
		}
		multiModuleWatcher.bsrClients[modules[i]] = watcher
	}

	return multiModuleWatcher, nil
}

func newSchemaWatcher(ctx context.Context, bsrUrl string, bsrApiKey string, module string, version string) (*prototransform.SchemaWatcher, error) {
	segments := strings.Split(module, "/")
	if len(segments) != 3 {
		return nil, fmt.Errorf("could not parse module %s, expected three segments e.g. 'buf.build/exampleco/payments'", module)
	}

	client := reflectv1beta1connect.NewFileDescriptorSetServiceClient(
		http.DefaultClient, "https://"+segments[0],
		connectrpc.WithInterceptors(prototransform.NewAuthInterceptor(bsrApiKey)),
		connectrpc.WithHTTPGet(),
		connectrpc.WithHTTPGetMaxURLSize(8192, true),
	)

	cfg := &prototransform.SchemaWatcherConfig{
		SchemaPoller: prototransform.NewSchemaPoller(
			client,
			module,
			version,
		),
	}
	watcher, err := prototransform.NewSchemaWatcher(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema watcher: %w", err)
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err = watcher.AwaitReady(ctxWithTimeout); err != nil {
		return nil, fmt.Errorf("schema watcher never became ready: %w", err)
	}

	return watcher, nil
}

func (w *MultiModuleWatcher) FindExtensionByName(field protoreflect.FullName) (protoreflect.ExtensionType, error) {
	for _, schemaWatcher := range w.bsrClients {
		extensionType, err := schemaWatcher.FindExtensionByName(field)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return extensionType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", field)
}

func (w *MultiModuleWatcher) FindExtensionByNumber(message protoreflect.FullName, field protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	for _, schemaWatcher := range w.bsrClients {
		extensionType, err := schemaWatcher.FindExtensionByNumber(message, field)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return extensionType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", message)
}

func (w *MultiModuleWatcher) FindMessageByName(message protoreflect.FullName) (protoreflect.MessageType, error) {
	for _, schemaWatcher := range w.bsrClients {
		messageType, err := schemaWatcher.FindMessageByName(message)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return messageType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", message)
}

func (w *MultiModuleWatcher) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	for _, schemaWatcher := range w.bsrClients {
		mesageType, err := schemaWatcher.FindMessageByURL(url)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return mesageType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", url)
}

func (w *MultiModuleWatcher) FindEnumByName(enum protoreflect.FullName) (protoreflect.EnumType, error) {
	for _, schemaWatcher := range w.bsrClients {
		enumType, err := schemaWatcher.FindEnumByName(enum)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return enumType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", enum)
}
