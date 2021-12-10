package sics.seiois.mlsserver.model.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Table(name = "mls_model")
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Model implements Serializable {

    private static final long serialVersionUID = -6632843229256239039L;

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "name")
    private String name;

    @Column(name = "type")
    private String type;

    @Column(name = "threshold")
    private double threshold;

    @Column(name = "tag")
    private String tag;

    @Column(name = "description")
    private String description;

    @Column(name = "version")
    private long version;

    @Column(name = "path")
    private String path;

    @Column(name = "warehouse")
    private String warehouse;

    @Column(name = "status")
    private float status;
}