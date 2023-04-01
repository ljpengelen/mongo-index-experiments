package nl.cofx.mongo.indices.experiment.operations;

public class IndexExistsWithDifferentName extends RuntimeException {

    public IndexExistsWithDifferentName(String message) {
        super(message);
    }
}
