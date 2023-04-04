package nl.cofx.mongo.indices.experiment;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * See {@link RandomDataIndexCreator} for additional ways to handle indexes for this entity.
 */
@Builder
@CompoundIndex(def = "{ randomString: 1, randomLong: 1 }", name = "idx0")
@Data
@Document
public class RandomData {

    @Indexed
    private String randomString;

    @Indexed
    private long randomLong;

    private boolean randomBoolean;
}
