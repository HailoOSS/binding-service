package healthcheck

import (
	"fmt"
	"github.com/HailoOSS/binding-service/util"
	instances "github.com/HailoOSS/discovery-service/proto/instances"
	"github.com/HailoOSS/platform/client"
	"github.com/HailoOSS/platform/server"
	"github.com/HailoOSS/service/healthcheck"
	"github.com/HailoOSS/platform-cli/rabbit"
	"github.com/HailoOSS/protobuf/proto"
	"sort"
	"strings"
)

const HealthCheckId = "com.HailoOSS.service.bindings"

// HealthCheck asserts all the local queues are bound that should be bound
func BindingHealthCheck() healthcheck.Checker {
	return checkBindings
}

//checks what is bound against what is returned in the discovery service
//only checks local bindings, not federated bindings
func checkBindings() (map[string]string, error) {

	errorMap := make(map[string]string)
	errorArr := sort.StringSlice{}

	//get rabbit host from disk (everytime, might be bad if called lots)
	rabMap, err := util.GetRabbitHosts()
	if err != nil {
		return nil, err
	}

	//Loop through az's
	for azname, hosts := range rabMap {

		ir := &instances.Request{
			AzName: proto.String(azname),
		}

		instancesReq, err := server.ScopedRequest("com.HailoOSS.kernel.discovery", "instances", ir)
		if err != nil {
			return nil, err
		}

		instancesRsp := &instances.Response{}

		// send the instances request
		if err := client.Req(instancesReq, instancesRsp); err != nil {
			return nil, err
		}

		//loop through rabbit hosts
		for _, rabbitHost := range hosts {

			rabbitBindings, err := rabbit.GetBindings("http://" + rabbitHost)
			if err != nil {
				return nil, err
			}

			//loop through discovery service instances
			for _, instance := range instancesRsp.GetInstances() {
				instId := instance.GetInstanceId()
				servName := instance.GetServiceName()

				//flag if instance is found and bound
				found := false

				//loop through rabbit bindings
				for _, rabbitBinding := range rabbitBindings {
					//check local bindings
					if rabbitBinding.Source == "h2o" {
						//check the routing key
						if rabbitBinding.Routing_key == servName {
							//check the destination
							if rabbitBinding.Destination == instId {
								found = true
							}
						}
					}
				}
				//Put the missing bindings into the error map
				if !found {
					errorMap[azname+"-rabbit."+rabbitHost+"-"+servName] = instId
					errorArr = append(errorArr, azname+"-rabbit."+rabbitHost+"-"+servName)
				}
			}
		}
	}

	if len(errorMap) != 0 {
		sort.Sort(errorArr)
		bindings := strings.Join(errorArr, ", ")
		if len(bindings) > 255 {
			bindings = bindings[:255]
		}
		return errorMap, fmt.Errorf("%d Inconsistant bindings: %s", len(errorArr), bindings)
	}

	return nil, nil
}
