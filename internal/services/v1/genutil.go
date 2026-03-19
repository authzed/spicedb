package v1

import (
	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// fakeServiceRegistrar implements the grpc.ServiceRegistrar interface
// so that we can interrogate the registered methods.
type fakeServiceRegistrar struct {
	// desc is a Service Description which we can interrogate
	desc *grpc.ServiceDesc
}

func (d *fakeServiceRegistrar) RegisterService(desc *grpc.ServiceDesc, impl any) {
	d.desc = desc
}

// inputMessagesForService takes a registration function for a service
// and the unimplemented service struct and returns a list of instantiated
// messages for those services. returns nil if it's unable to unpack the service.
// Used to pre-warm protovalidator caches.
func inputMessagesForService[S any](
	registerFn func(grpc.ServiceRegistrar, S),
	impl S,
) []proto.Message {
	registrar := &fakeServiceRegistrar{}
	registerFn(registrar, impl)

	desc, err := protoregistry.GlobalFiles.FindDescriptorByName(
		protoreflect.FullName(registrar.desc.ServiceName),
	)
	if err != nil {
		return nil
	}
	svcDesc := desc.(protoreflect.ServiceDescriptor)

	var msgs []proto.Message //nolint:prealloc  // we don't have any foreknowledge and also this isn't perf sensitive
	// iterate over Methods to capture unary services
	for _, md := range registrar.desc.Methods {
		methodDesc := svcDesc.Methods().ByName(protoreflect.Name(md.MethodName))
		msgType, err := protoregistry.GlobalTypes.FindMessageByName(methodDesc.Input().FullName())
		if err != nil {
			return nil
		}
		msgs = append(msgs, msgType.New().Interface())
	}
	// iterate over Streams to capture streaming services
	for _, sd := range registrar.desc.Streams {
		methodDesc := svcDesc.Methods().ByName(protoreflect.Name(sd.StreamName))
		msgType, err := protoregistry.GlobalTypes.FindMessageByName(methodDesc.Input().FullName())
		if err != nil {
			return nil
		}
		msgs = append(msgs, msgType.New().Interface())
	}
	return msgs
}

// AllServiceValidatorOptions returns protovalidate options that pre-warm the
// validator cache with input messages from all gRPC services. This allows a
// single shared validator to be used across all services.
func AllServiceValidatorOptions() []protovalidate.ValidatorOption {
	allMessages := make([]proto.Message, 0, 100)
	allMessages = append(allMessages, inputMessagesForService(v1.RegisterPermissionsServiceServer, v1.PermissionsServiceServer(nil))...)
	allMessages = append(allMessages, inputMessagesForService(v1.RegisterExperimentalServiceServer, v1.ExperimentalServiceServer(nil))...)
	allMessages = append(allMessages, inputMessagesForService(v1.RegisterSchemaServiceServer, v1.SchemaServiceServer(nil))...)
	allMessages = append(allMessages, inputMessagesForService(v1.RegisterWatchServiceServer, v1.WatchServiceServer(nil))...)
	return []protovalidate.ValidatorOption{protovalidate.WithMessages(allMessages...)}
}
