package model.dao;

import db.DB;
import model.dao.impl.DepartmentDaoJDBC;
import model.dao.impl.SellerDaoJDBC;

public class DaoFactory {

	public static SellerDao createSellerDao() {
		return new SellerDaoJDBC(DB.getConnection());
	}
	
	//Cria o construtor no DepartmentDaoJDBC primeiramente;
	public static DepartmentDaoJDBC createDepartmentDao() {
		return new DepartmentDaoJDBC(DB.getConnection());
	}
	
}