package com.wyu.dao;

import java.util.List;

import com.wyu.model.User;
import org.springframework.stereotype.Repository;

/**
 * 操作user的dao接口
 * 
 * @author ken
 *
 */
@Repository
public interface UserDao extends BaseDao<User> {

}

