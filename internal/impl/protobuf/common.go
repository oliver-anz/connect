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
	"errors"
	"fmt"

	"github.com/jhump/protoreflect/desc/protoparse"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// RegistriesFromMap attempts to parse a map of filenames (relative to import
// directories) and their contents out into a registry of protobuf files and
// protobuf types. These registries can then be used as a mechanism for
// dynamically (un)marshalling the definitions within.
func RegistriesFromMap(filesMap map[string]string) (*protoregistry.Files, *protoregistry.Types, error) {
	var parser protoparse.Parser
	parser.Accessor = protoparse.FileContentsFromMap(filesMap)

	names := make([]string, 0, len(filesMap))
	for k := range filesMap {
		names = append(names, k)
	}

	fds, err := parser.ParseFiles(names...)
	if err != nil {
		return nil, nil, err
	}

	files, types := &protoregistry.Files{}, &protoregistry.Types{}
	for _, v := range fds {
		if err := files.RegisterFile(v.UnwrapFile()); err != nil {
			return nil, nil, fmt.Errorf("failed to register file '%v': %w", v.GetName(), err)
		}
		for _, t := range v.GetMessageTypes() {
			if err := types.RegisterMessage(dynamicpb.NewMessageType(t.UnwrapMessage())); err != nil {
				return nil, nil, fmt.Errorf("failed to register type '%v': %w", t.GetName(), err)
			}
			for _, nt := range t.GetNestedMessageTypes() {
				if err := types.RegisterMessage(dynamicpb.NewMessageType(nt.UnwrapMessage())); err != nil {
					return nil, nil, fmt.Errorf("failed to register type '%v': %w", nt.GetFullyQualifiedName(), err)
				}
			}
		}
	}
	return files, types, nil
}

func RegistriesFromFileDescriptorSet(fd *descriptorpb.FileDescriptorSet) (*protoregistry.Files, *protoregistry.Types, error) {
	if len(fd.GetFile()) == 0 {
		return nil, nil, errors.New("") // todo
	}

	files, err := protodesc.FileOptions{AllowUnresolvable: true}.NewFiles(fd)
	if err != nil {
		return nil, nil, err // todo
	}

	types := &protoregistry.Types{}
	var rangeErr error
	files.RangeFiles(func(fileDescriptor protoreflect.FileDescriptor) bool {
		if err := registerTypes(types, fileDescriptor); err != nil {
			rangeErr = err
			return false
		}
		return true
	})
	if rangeErr != nil {
		return nil, nil, rangeErr // todo
	}
	return files, types, nil
}

type typeContainer interface {
	Enums() protoreflect.EnumDescriptors
	Messages() protoreflect.MessageDescriptors
	Extensions() protoreflect.ExtensionDescriptors
}

func registerTypes(types *protoregistry.Types, container typeContainer) error {
	for i := 0; i < container.Enums().Len(); i++ {
		if err := types.RegisterEnum(dynamicpb.NewEnumType(container.Enums().Get(i))); err != nil {
			return err
		}
	}
	for i := 0; i < container.Messages().Len(); i++ {
		msg := container.Messages().Get(i)
		if err := types.RegisterMessage(dynamicpb.NewMessageType(msg)); err != nil {
			return err
		}
		if err := registerTypes(types, msg); err != nil {
			return err
		}
	}
	for i := 0; i < container.Extensions().Len(); i++ {
		if err := types.RegisterExtension(dynamicpb.NewExtensionType(container.Extensions().Get(i))); err != nil {
			return err
		}
	}
	return nil
}
