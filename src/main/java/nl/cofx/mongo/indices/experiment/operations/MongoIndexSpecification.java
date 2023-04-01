package nl.cofx.mongo.indices.experiment.operations;

import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class MongoIndexSpecification {

    String definition;
    String name;
    boolean unique;
}
