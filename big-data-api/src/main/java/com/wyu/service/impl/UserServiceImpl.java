package com.wyu.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.JsonObject;
import com.wyu.dao.UserDao;
import com.wyu.model.User;
import com.wyu.service.UserService;

/**
 * @author ken
 */
@Service
public class UserServiceImpl implements UserService {

	@Autowired
	private UserDao userDao;

	@Override
	public JsonObject getOneUser() {
		JsonObject userJo = new JsonObject();
		User user = userDao.queryObject(1);
		userJo.addProperty("user",user.getName() );

		return userJo;
	}

}
