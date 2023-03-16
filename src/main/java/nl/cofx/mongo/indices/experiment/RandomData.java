package nl.cofx.mongo.indices.experiment;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Builder
@Data
@Document
@CompoundIndex(def = "{ randomString: 1, randomInt: 1 }", name = "idx0")
public class RandomData {

    @Indexed
    private String randomString;

    @Indexed
    private int randomInt;

    private boolean randomBoolean;
}
