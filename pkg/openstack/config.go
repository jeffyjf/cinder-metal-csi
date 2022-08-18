package openstack

// type controllerRequest interface {
// 	GetParameters() map[string]string
// 	GetSecrets() map[string]string
// }

// func NewAuthOptions(req *controllerRequest) *gophercloud.AuthOptions {
// 	paras := (*req).GetParameters()
// 	if paras["cinderAuthType"] == "keystone" {
// 		secrets := (*req).GetSecrets()
// 		return &gophercloud.AuthOptions{
// 			IdentityEndpoint: paras["osIdentityEndpoint"],
// 			Username:         paras["osUserName"],
// 			Password:         secrets["osUserPassword"],
// 			TenantName:       paras["osProject"],
// 			DomainName:       paras["osDomainName"],
// 			AllowReauth:      true,
// 		}
// 	} else {
// 		return &gophercloud.AuthOptions{}
// 	}
// }
