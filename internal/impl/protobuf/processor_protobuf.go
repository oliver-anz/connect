// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protobuf

import (
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"

	"buf.build/gen/go/bufbuild/reflect/connectrpc/go/buf/reflect/v1beta1/reflectv1beta1connect"
	reflectv1beta1 "buf.build/gen/go/bufbuild/reflect/protocolbuffers/go/buf/reflect/v1beta1"
	"github.com/bufbuild/prototransform"
	"github.com/redpanda-data/benthos/v4/public/service"

	connectrpc "connectrpc.com/connect"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	fieldOperator            = "operator"
	fieldMessage             = "message"
	fieldMessageFromMetadata = "message_from_metadata"
	fieldImportPaths         = "import_paths"
	fieldDiscardUnknown      = "discard_unknown"
	fieldUseProtoNames       = "use_proto_names"

	// BSR Config
	fieldBSRConfig = "bsr"
	fieldBsrUrl    = "url"
	fieldBsrApiKey = "api_key"
	fieldModule    = "module"
	fieldVersion   = "version"
)

func protobufProcessorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Parsing").
		Summary(`
Performs conversions to or from a protobuf message. This processor uses reflection, meaning conversions can be made directly from the target .proto files.
`).Description(`
The main functionality of this processor is to map to and from JSON documents, you can read more about JSON mapping of protobuf messages here: https://developers.google.com/protocol-buffers/docs/proto3#json[https://developers.google.com/protocol-buffers/docs/proto3#json^]

Using reflection for processing protobuf messages in this way is less performant than generating and using native code. Therefore when performance is critical it is recommended that you use Redpanda Connect plugins instead for processing protobuf messages natively, you can find an example of Redpanda Connect plugins at https://github.com/benthosdev/benthos-plugin-example[https://github.com/benthosdev/benthos-plugin-example^]

== Operators

=== `+"`to_json`"+`

Converts protobuf messages into a generic JSON structure. This makes it easier to manipulate the contents of the document within Benthos.

=== `+"`from_json`"+`

Attempts to create a target protobuf message from a generic JSON structure.
`).Fields(
		service.NewStringEnumField(fieldOperator, "to_json", "from_json").
			Description("The <<operators, operator>> to execute"),
		service.NewStringField(fieldMessage).
			Description("The fully qualified name of the protobuf message to convert to/from. Exclusive with message_from_metadata.").
			Default(""),
		service.NewStringField(fieldMessageFromMetadata).
			Description("The metadata attribute from which to read the fully qualified name of the protobuf message to convert to/from. Exclusive with message.").
			Default(""),
		service.NewBoolField(fieldDiscardUnknown).
			Description("If `true`, the `from_json` operator discards fields that are unknown to the schema.").
			Default(false),
		service.NewBoolField(fieldUseProtoNames).
			Description("If `true`, the `to_json` operator deserializes fields exactly as named in schema file.").
			Default(false),
		service.NewStringListField(fieldImportPaths).
			Description("A list of directories containing .proto files, including all definitions required for parsing the target message. If left empty the current directory is used. Each directory listed will be walked with all found .proto files imported. Field ignored if using Buf Schema Registry Reflection API").
			Default([]string{}),
		service.NewObjectListField(fieldBSRConfig,
			service.NewStringField(fieldBsrUrl).
				Description("Buf Schema Registry URL, leave blank to extract from module.").
				Default("").Advanced(),
			service.NewStringField(fieldBsrApiKey).
				Description("Buf Schema Registry API server API key, can be left blank for a public registry.").
				Secret().
				Default(""),
			service.NewStringField(fieldModule).
				Description("Module to fetch from Buf Schema Registry e.g. 'buf.build/exampleco/payments'.").
				Default(""),
			service.NewStringField(fieldVersion).
				Description("Version to retrieve from the Buf Schema Registry, leave blank for latest.").
				Default("").Advanced(),
		).Description("Optional Buf Schema Registry Reflection API config"),
	).Example(
		"JSON to Protobuf", `
If we have the following protobuf definition within a directory called `+"`testing/schema`"+`:

`+"```protobuf"+`
syntax = "proto3";
package testing;

import "google/protobuf/timestamp.proto";

message Person {
  string first_name = 1;
  string last_name = 2;
  string full_name = 3;
  int32 age = 4;
  int32 id = 5; // Unique ID number for this person.
  string email = 6;

  google.protobuf.Timestamp last_updated = 7;
}
`+"```"+`

And a stream of JSON documents of the form:

`+"```json"+`
{
	"firstName": "caleb",
	"lastName": "quaye",
	"email": "caleb@myspace.com"
}
`+"```"+`

We can convert the documents into protobuf messages with the following config:`, `
pipeline:
  processors:
    - protobuf:
        operator: from_json
        message: testing.Person
        import_paths: [ testing/schema ]
`).Example(
		"Protobuf to JSON", `
If we have the following protobuf definition within a directory called `+"`testing/schema`"+`:

`+"```protobuf"+`
syntax = "proto3";
package testing;

import "google/protobuf/timestamp.proto";

message Person {
  string first_name = 1;
  string last_name = 2;
  string full_name = 3;
  int32 age = 4;
  int32 id = 5; // Unique ID number for this person.
  string email = 6;

  google.protobuf.Timestamp last_updated = 7;
}
`+"```"+`

And a stream of protobuf messages of the type `+"`Person`"+`, we could convert them into JSON documents of the format:

`+"```json"+`
{
	"firstName": "caleb",
	"lastName": "quaye",
	"email": "caleb@myspace.com"
}
`+"```"+`

With the following config:`, `
pipeline:
  processors:
    - protobuf:
        operator: to_json
        message: testing.Person
        import_paths: [ testing/schema ]
`)
}

func init() {
	err := service.RegisterProcessor("protobuf", protobufProcessorSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newProtobuf(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type protobufOperator func(part *service.Message) error

func newProtobufToJSONOperator(f fs.FS, message, messageFromMetadata string, importPaths []string, useProtoNames bool) (protobufOperator, error) {
	descriptors, types, err := loadDescriptors(f, importPaths)
	if err != nil {
		return nil, err
	}

	return func(part *service.Message) error {
		msg, err := getMessage(part, message, messageFromMetadata)
		if err != nil {
			return err
		}

		d, err := descriptors.FindDescriptorByName(protoreflect.FullName(msg))
		if err != nil {
			return fmt.Errorf("unable to find message '%v' definition within '%v'", msg, importPaths)
		}

		md, ok := d.(protoreflect.MessageDescriptor)
		if !ok {
			return fmt.Errorf("message descriptor %v was unexpected type %T", msg, d)
		}

		partBytes, err := part.AsBytes()
		if err != nil {
			return err
		}

		dynMsg := dynamicpb.NewMessage(md)
		if err := proto.Unmarshal(partBytes, dynMsg); err != nil {
			return fmt.Errorf("failed to unmarshal protobuf message '%v': %w", msg, err)
		}

		opts := protojson.MarshalOptions{
			Resolver:      types,
			UseProtoNames: useProtoNames,
		}
		data, err := opts.Marshal(dynMsg)
		if err != nil {
			return fmt.Errorf("failed to marshal JSON protobuf message '%v': %w", msg, err)
		}

		part.SetBytes(data)
		return nil
	}, nil
}

func newProtobufToJSONBSROperator(multiModuleWatcher *MultiModuleWatcher, message, messageFromMetadata string, useProtoNames bool) (protobufOperator, error) {
	return func(part *service.Message) error {
		msg, err := getMessage(part, message, messageFromMetadata)
		if err != nil {
			return err
		}

		d, err := multiModuleWatcher.FindMessageByName(protoreflect.FullName(msg))
		if err != nil {
			return fmt.Errorf("unable to find message '%v' definition: %w", msg, err)
		}

		partBytes, err := part.AsBytes()
		if err != nil {
			return err
		}

		dynMsg := dynamicpb.NewMessage(d.Descriptor())
		if err := proto.Unmarshal(partBytes, dynMsg); err != nil {
			return fmt.Errorf("failed to unmarshal protobuf message '%v': %w", msg, err)
		}

		opts := protojson.MarshalOptions{
			Resolver:      multiModuleWatcher,
			UseProtoNames: useProtoNames,
		}
		data, err := opts.Marshal(dynMsg)
		if err != nil {
			return fmt.Errorf("failed to marshal JSON protobuf message '%v': %w", msg, err)
		}

		part.SetBytes(data)
		return nil
	}, nil
}

func newProtobufFromJSONOperator(f fs.FS, message, messageFromMetadata string, importPaths []string, discardUnknown bool) (protobufOperator, error) {
	_, types, err := loadDescriptors(f, importPaths)
	if err != nil {
		return nil, err
	}

	// types.RangeMessages(func(mt protoreflect.MessageType) bool {
	// 	return true
	// })

	return func(part *service.Message) error {
		msg, err := getMessage(part, message, messageFromMetadata)
		if err != nil {
			return err
		}

		md, err := types.FindMessageByName(protoreflect.FullName(msg))
		if err != nil {
			return fmt.Errorf("unable to find message '%v' definition within '%v'", msg, importPaths)
		}

		msgBytes, err := part.AsBytes()
		if err != nil {
			return err
		}

		dynMsg := dynamicpb.NewMessage(md.Descriptor())

		opts := protojson.UnmarshalOptions{
			Resolver:       types,
			DiscardUnknown: discardUnknown,
		}
		if err := opts.Unmarshal(msgBytes, dynMsg); err != nil {
			return fmt.Errorf("failed to unmarshal JSON message '%v': %w", msg, err)
		}

		data, err := proto.Marshal(dynMsg)
		if err != nil {
			return fmt.Errorf("failed to marshal protobuf message '%v': %v", msg, err)
		}

		part.SetBytes(data)
		return nil
	}, nil
}

func newProtobufFromJSONBSROperator(multiModuleWatcher *MultiModuleWatcher, message, messageFromMetadata string, discardUnknown bool) (protobufOperator, error) {
	return func(part *service.Message) error {
		msg, err := getMessage(part, message, messageFromMetadata)
		if err != nil {
			return err
		}
		
		d, err := multiModuleWatcher.FindMessageByName(protoreflect.FullName(msg))
		if err != nil {
			return fmt.Errorf("unable to find message '%v' definition: %w", msg, err)
		}

		msgBytes, err := part.AsBytes()
		if err != nil {
			return err
		}

		dynMsg := dynamicpb.NewMessage(d.Descriptor())

		opts := protojson.UnmarshalOptions{
			Resolver:       multiModuleWatcher,
			DiscardUnknown: discardUnknown,
		}
		if err := opts.Unmarshal(msgBytes, dynMsg); err != nil {
			return fmt.Errorf("failed to unmarshal JSON message '%v': %w", msg, err)
		}

		data, err := proto.Marshal(dynMsg)
		if err != nil {
			return fmt.Errorf("failed to marshal protobuf message '%v': %v", msg, err)
		}

		part.SetBytes(data)
		return nil
	}, nil
}

func strToProtobufOperator(f fs.FS, opStr, message, messageFromMetadata string, importPaths []string, discardUnknown, useProtoNames bool) (protobufOperator, error) {
	switch opStr {
	case "to_json":
		return newProtobufToJSONOperator(f, message, messageFromMetadata, importPaths, useProtoNames)
	case "from_json":
		return newProtobufFromJSONOperator(f, message, messageFromMetadata, importPaths, discardUnknown)
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

func strToProtobufBSROperator(multiModuleWatcher *MultiModuleWatcher, opStr, message, messageFromMetadata string, discardUnknown, useProtoNames bool) (protobufOperator, error) {
	switch opStr {
	case "to_json":
		return newProtobufToJSONBSROperator(multiModuleWatcher, message, messageFromMetadata, useProtoNames)
	case "from_json":
		return newProtobufFromJSONBSROperator(multiModuleWatcher, message, messageFromMetadata, discardUnknown)
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

func getMessage(part *service.Message, message, messageFromMetadata string) (string, error) {
	// use static message name
	if message != "" {
		return message, nil
	} else {
		// else retrieve from metadata
		value, present := part.MetaGetMut(messageFromMetadata)
		if !present {
			return "", fmt.Errorf("could not read value for messageFromMetadata key %s", messageFromMetadata)
		}
		msg, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("value for messageFromMetadata key %s was not a string", messageFromMetadata)
		}
		return msg, nil
	}
}

func loadDescriptors(f fs.FS, importPaths []string) (*protoregistry.Files, *protoregistry.Types, error) {
	files := map[string]string{}
	for _, importPath := range importPaths {
		if err := fs.WalkDir(f, importPath, func(path string, info fs.DirEntry, ferr error) error {
			if ferr != nil || info.IsDir() {
				return ferr
			}
			if filepath.Ext(info.Name()) == ".proto" {
				rPath, ferr := filepath.Rel(importPath, path)
				if ferr != nil {
					return fmt.Errorf("failed to get relative path: %v", ferr)
				}
				content, ferr := os.ReadFile(path)
				if ferr != nil {
					return fmt.Errorf("failed to read import %v: %v", path, ferr)
				}
				files[rPath] = string(content)
			}
			return nil
		}); err != nil {
			return nil, nil, err
		}
	}
	return RegistriesFromMap(files)
}

type bsrConfig struct {
	reflectionServerURL, reflectionServerAPIKey, module, version string
}

func loadDescriptorsFromBSR(c *bsrConfig) (*protoregistry.Files, *protoregistry.Types, error) {
	client := reflectv1beta1connect.NewFileDescriptorSetServiceClient(
		http.DefaultClient, c.reflectionServerURL,
		connectrpc.WithInterceptors(prototransform.NewAuthInterceptor(c.reflectionServerAPIKey)),
		connectrpc.WithHTTPGet(),
		connectrpc.WithHTTPGetMaxURLSize(8192, true),
	)

	req := connectrpc.NewRequest(&reflectv1beta1.GetFileDescriptorSetRequest{
		Module:  c.module,
		Version: c.version,
	})

	res, err := client.GetFileDescriptorSet(context.Background(), req)
	if err != nil {
		return nil, nil, fmt.Errorf("could not retrieve file descriptor set %v", err.Error())
	}
	if len(res.Msg.GetFileDescriptorSet().GetFile()) == 0 {
		return nil, nil, fmt.Errorf("empty file descriptor set for module and version")
	}
	return RegistriesFromFileDescriptorSet(res.Msg.GetFileDescriptorSet())
}

// ------------------------------------------------------------------------------

type protobufProc struct {
	operator protobufOperator
	log      *service.Logger
	// Used for loading schemas from buf schema registries
	multiModuleWatcher *MultiModuleWatcher
}

func newProtobuf(conf *service.ParsedConfig, mgr *service.Resources) (*protobufProc, error) {
	p := &protobufProc{
		log: mgr.Logger(),
	}

	operatorStr, err := conf.FieldString(fieldOperator)
	if err != nil {
		return nil, err
	}

	var message string
	if message, err = conf.FieldString(fieldMessage); err != nil {
		return nil, err
	}

	var messageFromMetadata string
	if messageFromMetadata, err = conf.FieldString(fieldMessageFromMetadata); err != nil {
		return nil, err
	}

	if messageFromMetadata == "" && message == "" {
		return nil, fmt.Errorf("message and message_from_metadata can't both be empty")
	}

	if messageFromMetadata != "" && message != "" {
		return nil, fmt.Errorf("message and message_from_metadata can't both be populated")
	}

	var discardUnknown bool
	if discardUnknown, err = conf.FieldBool(fieldDiscardUnknown); err != nil {
		return nil, err
	}

	var useProtoNames bool
	if useProtoNames, err = conf.FieldBool(fieldUseProtoNames); err != nil {
		return nil, err
	}

	// Load BSR config
	var bsrModules []*service.ParsedConfig
	if bsrModules, err = conf.FieldObjectList(fieldBSRConfig); err != nil {
		return nil, err
	}

	// if BSR config is present, use BSR to discover proto definitions
	if len(bsrModules) > 0 {
		p.multiModuleWatcher, err = newMultiModuleWatcher(bsrModules)
		if err != nil {
			return nil, fmt.Errorf("failed to create MultiModuleWatcher: %w", err)
		}

		if p.operator, err = strToProtobufBSROperator(p.multiModuleWatcher, operatorStr, message, messageFromMetadata, discardUnknown, useProtoNames); err != nil {
			return nil, err
		}
	} else {
		// else read from file paths
		var importPaths []string
		if importPaths, err = conf.FieldStringList(fieldImportPaths); err != nil {
			return nil, err
		}

		if p.operator, err = strToProtobufOperator(mgr.FS(), operatorStr, message, messageFromMetadata, importPaths, discardUnknown, useProtoNames); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (p *protobufProc) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	if err := p.operator(msg); err != nil {
		p.log.Debugf("Operator failed: %v", err)
		return nil, err
	}
	return service.MessageBatch{msg}, nil
}

func (p *protobufProc) Close(context.Context) error {
	return nil
}
