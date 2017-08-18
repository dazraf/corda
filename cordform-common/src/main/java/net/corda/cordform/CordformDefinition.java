package net.corda.cordform;

import org.bouncycastle.asn1.x500.X500Name;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public abstract class CordformDefinition {
    public final Path driverDirectory;
    public final ArrayList<Consumer<? super CordformNode>> nodeConfigurers = new ArrayList<>();
    public final X500Name networkMapNodeName;

    public CordformDefinition(Path driverDirectory, X500Name networkMapNodeName) {
        this.driverDirectory = driverDirectory;
        this.networkMapNodeName = networkMapNodeName;
    }

    public void addNode(Consumer<? super CordformNode> configurer) {
        nodeConfigurers.add(configurer);
    }

    /**
     * Make arbitrary changes to the node directories before they are started.
     * @param context Lookup of node directory by node name.
     */
    public abstract void setup(List<CordformNode> nodes, CordformContext context);
}
