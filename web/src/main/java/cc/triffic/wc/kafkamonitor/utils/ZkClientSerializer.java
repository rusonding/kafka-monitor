package cc.triffic.wc.kafkamonitor.utils;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

/**
 * Created by lixun on 2017/3/16.
 */
public class ZkClientSerializer implements ZkSerializer {
    @Override
    public byte[] serialize(Object o) throws ZkMarshallingError {
        return new byte[0];
    }

    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        if (bytes == null) {
            return null;
        } else {
            try {
                return new String(bytes, "UTF-8");
            } catch (Exception e) {

            }
            return null;
        }
    }
}
