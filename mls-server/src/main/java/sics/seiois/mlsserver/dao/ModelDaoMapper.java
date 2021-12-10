package sics.seiois.mlsserver.dao;


import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import sics.seiois.mlsserver.model.entity.Model;

import java.util.List;

@Mapper
public interface ModelDaoMapper {

    boolean save(Model model);

    Model findById(String id);

    List<Model> listByName(@Param("name") String name);

    Long findVersionByName(String name);

    boolean deleteById(String id);

    String findByStatus(String id);

    boolean updateFail(String id);

    boolean updateFini(String id);

    boolean updateSucc(@Param("status") float status, @Param("id") String id);

    boolean addtmTask(@Param("taskid") String taskid, @Param("id") String id);

    Float listStatus(String id);

    List<String> getPath(String name);

    Float getStatus(String id);

    boolean updateStatus(@Param("id") String id, @Param("status") float status);

    String findWarehouseById(String id);

    List<Model> list(@Param("name") String name, @Param("type") String type, @Param("tag") String tag, @Param("description") String description, @Param("pageNum") Integer pageNum, @Param("pageSize") Integer pageSize);
}