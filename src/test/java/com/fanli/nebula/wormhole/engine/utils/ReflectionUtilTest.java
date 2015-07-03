package com.fanli.nebula.wormhole.engine.utils;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fanli.nebula.wormhole.common.DefaultLine;
import com.fanli.nebula.wormhole.engine.common.TestUtils;
import org.junit.Before;
import org.junit.Test;

import com.fanli.nebula.wormhole.common.interfaces.ILine;

public class ReflectionUtilTest {
	
	private JarLoader jarLoader;
	private String className;
	
	@Before
	public void init(){
		jarLoader  = JarLoader.getInstance(
				new String[] {getPath(new String[] {"jar", "path01"}),
						  getPath(new String[] {"jar", "path02"})});
		className = "com.dianping.ls.analysis.common.CommonAnalysisUtils";
		
	}
	
	@Test
	public void testCreateInstanceByDefaultConstructor_with_class_loader_Success(){
		Object o = ReflectionUtil.createInstanceByDefaultConstructor(className, Object.class, jarLoader);
		assertNotNull(o);
	}
	
	@Test
	public void testCreateInstanceByDefaultConstructor_with_class_loader_Failed(){
		Object o = ReflectionUtil.createInstanceByDefaultConstructor("aaaaa", Object.class, jarLoader);
		assertNull(o);
	}
	
	@Test
	public void testCreateInstanceByDefaultConstructor_without_class_loader_Success(){
		DefaultLine line = ReflectionUtil.createInstanceByDefaultConstructor("com.dp.nebula.wormhole.common.DefaultLine", DefaultLine.class);
		assertNotNull(line);
		assertTrue(line instanceof ILine);
		assertTrue(line instanceof DefaultLine);
	}
	
	@Test
	public void testCreateInstanceByDefaultConstructor_without_class_loader_Failed(){
		Object o = ReflectionUtil.createInstanceByDefaultConstructor(className, Object.class);
		assertNull(o);
	}
	
	
	private String getPath(String[] paths){
		return TestUtils.getResourcePath(paths);
	}

}
