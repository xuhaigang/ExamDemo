package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.*;

/*
*类名和方法不能修改
 */
public class Schedule {

	/** 注册的服务节点集合 */
	List<Integer> nodeList = new ArrayList<>();
	/** 添加的所有任务集合 */
	Map<Integer, Integer> taskMap = new LinkedHashMap<Integer, Integer>();
	/** 服务节点关系集合 */
	List<TaskInfo> taskInfoList = new ArrayList<>();
	List<Integer> taskList = new ArrayList<>();
	private Map<Integer,List<TaskInfo>> nodeTaskList = new HashMap<Integer, List<TaskInfo>>();
	private Map<Integer,List<Integer>> tempTaskList = new HashMap<Integer, List<Integer>>();

	private static int threshold;

	Comparator<TaskInfo> comparator = new Comparator<TaskInfo>(){
		public int compare(TaskInfo o1, TaskInfo o2) {
			return (o1.getTaskId()-o2.getTaskId());
		}
	};

	Comparator<TaskInfo> comparatorByNodeId = new Comparator<TaskInfo>(){
		public int compare(TaskInfo o1, TaskInfo o2) {
			return (o1.getNodeId()-o2.getNodeId());
		}
	};

	Comparator<Integer> comparatorByTime = new Comparator<Integer>(){
		public int compare(Integer o1, Integer o2) {
			return (taskMap.get(o2)-taskMap.get(o1));
		}
	};


    public int init() {
		nodeList.clear();
		taskMap.clear();
		taskInfoList.clear();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
    	if (nodeId > 0) {
    		if (nodeList.contains(nodeId)) {
				return ReturnCodeKeys.E005;
			} else {
				nodeList.add(nodeId);
				return ReturnCodeKeys.E003;
			}

		} else {
			return ReturnCodeKeys.E004;
		}

    }

    public int unregisterNode(int nodeId) {
		if (nodeId > 0) {
			if (nodeList.contains(nodeId)) {
				Integer taskId = checkNodeHasTask(nodeId);
				if (taskId != null) {
					// TODO 服务节点正运行任务，则将运行的任务移到任务挂起队列中，等待调度程序调度。

				}
				for (int i = 0; i < nodeList.size(); i++) {
					if (nodeList.get(i) == nodeId) {
						nodeList.remove(i);
					}

				}

				return ReturnCodeKeys.E006;
			} else {
				return ReturnCodeKeys.E007;
			}
		} else {
			return ReturnCodeKeys.E004;
		}
    }


    public int addTask(int taskId, int consumption) {
    	if (taskId > 0) {
			if (taskMap.containsKey(taskId)) {
				return ReturnCodeKeys.E010;
			}
			taskList.add(taskId);
			taskMap.put(taskId,consumption);
			return ReturnCodeKeys.E008;
		} else {
			return ReturnCodeKeys.E009;
		}


    }


    public int deleteTask(int taskId) {
        if (taskId <= 0) {
			return ReturnCodeKeys.E009;
		}
		if (taskMap.containsKey(taskId)) {
        	taskMap.remove(taskId);
        	return ReturnCodeKeys.E011;
		}
        return ReturnCodeKeys.E012;
    }


    public int scheduleTask(int threshold) {
        if (threshold < 0) {
			return ReturnCodeKeys.E002;
		}

		boolean flag = false;
        this.threshold = threshold;

		List<Integer> tmpTasks = new ArrayList<Integer>();
		for(Integer taskId : taskList){
			tmpTasks.add(taskId);
		}
		for(Integer nodeId : nodeList){
			List<TaskInfo> taskInfos = new ArrayList<TaskInfo>();
			nodeTaskList.put(nodeId,taskInfos);
		}

		while(!flag || tmpTasks.size()>0){
			for(Integer taskId:tmpTasks){
				int tmpId = -1;
				int min = Integer.MAX_VALUE;
				for(Integer nodeId : nodeList){
					List<TaskInfo> taskInfos = nodeTaskList.get(nodeId);
					if(taskInfos == null){
						return nodeId;
					}else{
						int w = getTasksount(taskInfos);
						if(w < min){
							min = w;
							tmpId = nodeId;
						}
					}
				}

				List<TaskInfo> taskInfos = nodeTaskList.get(tmpId);
				TaskInfo taskInfo = new TaskInfo();
				taskInfo.setTaskId(taskId);
				taskInfo.setNodeId(tmpId);
				taskInfos.add(taskInfo);
				tmpTasks.remove(new Integer(taskId));
				getTempTaskList(taskId);
				flag = checkBalance(tmpId);
				break;
			}
			if(tmpTasks.size()==0 && !flag) {
				return ReturnCodeKeys.E014;
			}



		}

		for (Integer time : tempTaskList.keySet()){
			List<Integer> list = tempTaskList.get(time);
			if(list.size()>1){

				List<TaskInfo> tasks = new ArrayList<TaskInfo>();
				for(Integer nodeId : nodeTaskList.keySet()){
					List<TaskInfo> taskInfos = nodeTaskList.get(nodeId);
					for(TaskInfo ti:taskInfos){
						if(list.contains(ti.getTaskId())){
							tasks.add(ti);
						}
					}
				}
				Collections.sort(tasks, comparatorByNodeId);
				Collections.sort(list);
				for(int i=0;i<tasks.size();i++){
					TaskInfo ti = tasks.get(i);
					ti.setTaskId(list.get(i));
				}
			}
		}

		return ReturnCodeKeys.E013;


    }


    public int queryTaskStatus(List<TaskInfo> tasks) {

		for(Integer nodeId : nodeTaskList.keySet()){
			tasks.addAll(nodeTaskList.get(nodeId));
		}
		Collections.sort(tasks,comparator);
		System.out.println(tasks);
		return ReturnCodeKeys.E015;
    }

    private Integer checkNodeHasTask(int nodeId) {
    	if (taskInfoList.size() > 0) {
			for (TaskInfo taskInfo : taskInfoList) {
				if (nodeId == taskInfo.getNodeId()) {
					return taskInfo.getTaskId();
				} else {
					return null;
				}
			}
		} else {
    		return null;
		}
		return null;
	}

	private int getTasksount(List<TaskInfo> taskInfoList){
		int result = 0;
		for(TaskInfo taskInfo : taskInfoList){
			result += taskMap.get(taskInfo.getTaskId());
		}
		return result;
	}

	private void getTempTaskList(int taskId){
		int time = taskMap.get(taskId);
		List<Integer> list = tempTaskList.get(time);
		if(list==null){
			list = new ArrayList<Integer>();
			tempTaskList.put(time,list);
		}
		list.add(taskId);
	}

	private boolean checkBalance(int nodeId){
		int source = getTasksount(nodeTaskList.get(nodeId));
		for(Integer id : nodeList){
			if(!id.equals(nodeId)){
				int t = 0;
				if(nodeTaskList.get(id)==null){
					t=0;
				}else{
					t = getTasksount(nodeTaskList.get(id));
				}

				if(Math.abs(t-source) > this.threshold) return false;
			}
		}
		return true;
	}


}
