package nl.cofx.mongo.indices.experiment.operations;

public class ExistingIndexHasSameName extends RuntimeException {

    public ExistingIndexHasSameName(String message) {
        super(message);
    }
}
