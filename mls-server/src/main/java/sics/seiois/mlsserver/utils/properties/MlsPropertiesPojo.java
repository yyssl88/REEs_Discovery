package sics.seiois.mlsserver.utils.properties;

import com.google.common.base.MoreObjects;
import lombok.*;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;

/**
 * Copy From EI
 */
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@Getter
@Setter
@Table(name = "mls.mls_properties")
public class MlsPropertiesPojo implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    @Column(name = "key")
    private String key;
    @Column(name = "value")
    private String value;
    @Column(name = "remark")
    private String remark;

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).omitNullValues()
                .add("key", key)
                .add("value", value)
                .add("remark", remark)
                .toString();
    }
}