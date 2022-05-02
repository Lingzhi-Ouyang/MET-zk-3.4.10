package org.mpisws.hitmc.api.state;

public interface PropertyVerifier<G extends GlobalState> {

    void verify(G globalState);

}
