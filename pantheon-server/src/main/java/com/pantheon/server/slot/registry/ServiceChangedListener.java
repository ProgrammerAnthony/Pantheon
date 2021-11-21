package com.pantheon.server.slot.registry;


import java.util.ArrayList;
import java.util.List;

/**
 * when service changed, get notified in {@link ServiceChangedListener#onChange(String, List)}
 */
public class ServiceChangedListener {

    /**
     * identifier for client connection
     */
    private String clientConnectionId;

    public ServiceChangedListener(String clientConnectionId) {
        this.clientConnectionId = clientConnectionId;
    }

    /**
     * notify this method when service changed
     *
     * @param serviceInstances
     */
    public void onChange(String serviceName, List<ServiceInstance> serviceInstances) {
        List<String> serviceInstanceAddresses = new ArrayList<String>();
        for (ServiceInstance serviceInstance : serviceInstances) {
            serviceInstanceAddresses.add(serviceInstance.getAddress());
        }

// build and send a request of service changed to client connection
//        Request request = new ServiceChangedRequest.Builder()
//                .serviceName(serviceName)
//                .serviceInstanceAddresses(serviceInstanceAddresses)
//                .build();

//        ClientMessageQueues clientRequestQueues = ClientMessageQueues.getInstance();
//        clientRequestQueues.offerMessage(clientConnectionId, request);
    }

}
