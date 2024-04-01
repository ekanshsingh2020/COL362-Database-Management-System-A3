package optimizer.rel;

import manager.StorageManager;

import java.util.List;

import org.apache.calcite.rel.RelNode;

public interface PRel extends RelNode {   
    public List <Object []> evaluate(StorageManager storage_manager);
}