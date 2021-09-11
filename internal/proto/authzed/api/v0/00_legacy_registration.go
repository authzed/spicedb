package v0

import "google.golang.org/grpc"

// RegisterACLServiceServer registers an ACLService under its legacy
// package-less descriptor.
func RegisterLegacyACLServiceServer(s grpc.ServiceRegistrar, srv ACLServiceServer) {
	s.RegisterService(&Legacy_ACLService_ServiceDesc, srv)
}

// Legacy_ACLService_ServiceDesc is an grpc.ServiceDesc for the original
// ACLService service that had no Protobuf package.
//
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Legacy_ACLService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "ACLService",
	HandlerType: (*ACLServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Read",
			Handler:    _ACLService_Read_Handler,
		},
		{
			MethodName: "Write",
			Handler:    _ACLService_Write_Handler,
		},
		{
			MethodName: "Check",
			Handler:    _ACLService_Check_Handler,
		},
		{
			MethodName: "ContentChangeCheck",
			Handler:    _ACLService_ContentChangeCheck_Handler,
		},
		{
			MethodName: "Expand",
			Handler:    _ACLService_Expand_Handler,
		},
		{
			MethodName: "Lookup",
			Handler:    _ACLService_Lookup_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "authzed/api/v0/acl_service.proto",
}

// RegisterLegacyNamespaceServiceServer registers a NamespaceService under
// its legacy package-less descriptor.
func RegisterLegacyNamespaceServiceServer(s grpc.ServiceRegistrar, srv NamespaceServiceServer) {
	s.RegisterService(&Legacy_NamespaceService_ServiceDesc, srv)
}

// Legacy_NamespaceService_ServiceDesc is an grpc.ServiceDesc for the original
// NamespaceService service that had no Protobuf package.
//
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Legacy_NamespaceService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "NamespaceService",
	HandlerType: (*NamespaceServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ReadConfig",
			Handler:    _NamespaceService_ReadConfig_Handler,
		},
		{
			MethodName: "WriteConfig",
			Handler:    _NamespaceService_WriteConfig_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "authzed/api/v0/namespace_service.proto",
}
