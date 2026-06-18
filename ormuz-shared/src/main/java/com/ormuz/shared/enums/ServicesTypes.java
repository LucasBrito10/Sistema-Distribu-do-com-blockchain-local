package com.ormuz.shared.enums;

import java.util.Arrays;
import java.util.List;


// Cada serviço está diretamente vinculado a um tipo específico de recurso e tópico, enums são vistos pela JVM como objetos fixos, 
// eles podem ter construtores e atributos.
public enum ServicesTypes {

    //Todas as próximas constantes são construtores de enum que recebem um tipo de recurso relacionado.
    

    // Serviços para COASTAL_RADAR
    TRAFFIC_MONITORING(ResourcesTypes.COASTAL_RADAR, TopicType.MARITIME_TRAFFIC),
    INTRUSION_DETECTION(ResourcesTypes.COASTAL_RADAR, TopicType.SECURITY_ALERTS),
    LONG_RANGE_SURVEILLANCE(ResourcesTypes.COASTAL_RADAR, TopicType.SECURITY_ALERTS),

    // Serviços para NAVAL_SENSORS
    HYDROGRAPHIC_PROFILING(ResourcesTypes.NAVAL_SENSORS, TopicType.ENVIRONMENTAL_TELEMETRY),
    PRESSURE_SENSING(ResourcesTypes.NAVAL_SENSORS, TopicType.ENVIRONMENTAL_TELEMETRY),

    // Serviços para SMART_BUOYS
    WATER_QUALITY_ANALYSIS(ResourcesTypes.SMART_BUOYS, TopicType.ENVIRONMENTAL_TELEMETRY),
    AIS_RELAY(ResourcesTypes.SMART_BUOYS, TopicType.MARITIME_TRAFFIC),

    // Serviços para DRONES
    VISUAL_RECONNAISSANCE(ResourcesTypes.DRONES, TopicType.MULTIMEDIA_STREAM),
    SEARCH_AND_RESCUE(ResourcesTypes.DRONES, TopicType.SECURITY_ALERTS),
    INFRASTRUCTURE_INSPECTION(ResourcesTypes.DRONES, TopicType.MAINTENANCE_LOGS);

    private final ResourcesTypes relatedResource;
    private final TopicType defaultTopic; 

    
    ServicesTypes(ResourcesTypes relatedResource, TopicType defaultTopic) {
        this.relatedResource = relatedResource;
        this.defaultTopic = defaultTopic;
    }

    public ResourcesTypes getRelatedResource() {
        return relatedResource;
    }

    public TopicType getDefaultTopic() {
        return defaultTopic;
    }

    // Retorna todos os serviços vinculados a um tipo específico de recurso.
    public static List<ServicesTypes> getServicesByResource(ResourcesTypes resource) {
        return Arrays.stream(ServicesTypes.values())
                     .filter(service -> service.getRelatedResource() == resource)
                     .toList();
    }
}