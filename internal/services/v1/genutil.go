package v1

import (
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type descCapture struct {
	desc *grpc.ServiceDesc
}

func (d *descCapture) RegisterService(desc *grpc.ServiceDesc, impl any) {
	d.desc = desc
}

// inputMessagesForService takes a registration function for a service
// and the unimplemented service struct and returns a list of instantiated
// messages for those services. Used to pre-warm protovalidator caches.
func inputMessagesForService[S any](
	registerFn func(grpc.ServiceRegistrar, S),
	impl S,
) ([]proto.Message) {
	cap := &descCapture{}
	registerFn(cap, impl)

	desc, err := protoregistry.GlobalFiles.FindDescriptorByName(
		protoreflect.FullName(cap.desc.ServiceName),
	)
	if err != nil {
		return nil
	}
	svcDesc := desc.(protoreflect.ServiceDescriptor)

	var msgs []proto.Message
	for _, md := range cap.desc.Methods {
		methodDesc := svcDesc.Methods().ByName(protoreflect.Name(md.MethodName))
		msgType, err := protoregistry.GlobalTypes.FindMessageByName(methodDesc.Input().FullName())
		if err != nil {
			return nil
		}
		msgs = append(msgs, msgType.New().Interface())
	}
	return msgs
}
