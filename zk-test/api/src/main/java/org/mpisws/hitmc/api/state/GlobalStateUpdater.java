package org.mpisws.hitmc.api.state;

public interface GlobalStateUpdater<G extends GlobalState> {

    void update(G globalState);

}
