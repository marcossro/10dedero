package es.us.dad.mysql.rest;

import java.util.Calendar;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import es.us.dad.mysql.entities.Actuator;
import es.us.dad.mysql.entities.ActuatorStatus;
import es.us.dad.mysql.entities.ActuatorType;
import es.us.dad.mysql.entities.Device;
import es.us.dad.mysql.entities.Group;
import es.us.dad.mysql.entities.Sensor;
import es.us.dad.mysql.entities.SensorType;
import es.us.dad.mysql.entities.SensorValue;
import es.us.dad.mysql.messages.DatabaseEntity;
import es.us.dad.mysql.messages.DatabaseMessage;
import es.us.dad.mysql.messages.DatabaseMessageIdAndActuatorType;
import es.us.dad.mysql.messages.DatabaseMessageIdAndSensorType;
import es.us.dad.mysql.messages.DatabaseMessageLatestValues;
import es.us.dad.mysql.messages.DatabaseMessageType;
import es.us.dad.mysql.messages.DatabaseMethod;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

public class RestAPIVerticle extends AbstractVerticle {

	private transient Gson gson;

	@Override
	public void start(Promise<Void> startFuture) {

		// Instantiating a Gson serialize object using specific date format
		gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();

		// Defining the router object
		Router router = Router.router(vertx);

		// Handling any server startup result
		HttpServer httpServer = vertx.createHttpServer();
		httpServer.requestHandler(router::handle).listen(81, result -> {
			if (result.succeeded()) {
				System.out.println("API Rest is listening on port 81");
				startFuture.complete();
			} else {
				startFuture.fail(result.cause());
			}
		});

		// Defining URI paths for each method in RESTful interface, including body
		// handling
		router.route("/api*").handler(BodyHandler.create());

		//-------------------------------Groups-------------------------------------------------------------------
				//GetGroup
				router.get("/api/groups/:groupid").handler(this::getGrupoById);
				//AddGroup
				router.post("/api/groups").handler(this::addGroups);
				//DeleteGroup
				router.delete("/api/groups/:groupid").handler(this::deleteGroup);
				//PutGroup
				router.put("/api/groups/:groupid").handler(this::putGroup);
			//GetDevicesFromGroupId
				router.get("/api/devicesFromGroup/:groupid").handler(this::GetDevicesFromGroupId);
		//-------------------------------Devices-------------------------------------------------------------------
				//GetDevice
				router.get("/api/devices/:deviceid").handler(this::getDeviceById);
				//AddDevice
				router.post("/api/devices").handler(this::addDevice);
				//DeleteDevice
				router.delete("/api/devices/:deviceid").handler(this::deleteDevice);
				//PutDevice
				router.put("/api/devices/:deviceid").handler(this::putDevice);
				//GetSensorsFromDeviceId
				router.get("/api/devicesSensor/:deviceid").handler(this::getSensorsFromDeviceId);
				//GetActuatorsFromDeviceId
				router.get("/api/devicesActuator/:deviceid").handler(this::getActuatorsFromDeviceId);
				//GetSensorsFromDeviceIdAndSensorType
				router.get("/api/devicesSensorType/:deviceid/:sensortype").handler(this::GetSensorsFromDeviceIdAndSensorType);
				//GetActuatorsFromDeviceIdAndActuatorType
				router.get("/api/devicesActuatorType/:deviceid/:actuatortype").handler(this::GetActuatorsFromDeviceIdAndActuatorType);
			//AddDeviceToGroup
				router.put("/api/addDevicesToGroup/:groupid/:deviceid").handler(this::AddDeviceToGroup);
		//-------------------------------Sensors-------------------------------------------------------------------
				//GetSensor
				router.get("/api/sensors/:sensorid").handler(this::getSensorById);
				//AddSensor
				router.post("/api/sensors").handler(this::addSensor);
				//DeleteSensor
				router.delete("/api/sensors/:sensorid").handler(this::deleteSensor);
				//PutSensor
				router.put("/api/sensors/:sensorid").handler(this::putSensor);
		//-------------------------------Actuators-----------------------------------------------------------------
				//GetActuator
				router.get("/api/actuators/:actuatorid").handler(this::getActuatorById);
				//AddActuator
				router.post("/api/actuators").handler(this::addActuator);
				//DeleteActuator
				router.delete("/api/actuators/:actuatorid").handler(this::deleteActuator);
				//PutActuator
				router.put("/api/actuators/:actuatorid").handler(this::putActuator);
		//-------------------------------Sensors Values-----------------------------------------------------------------
				//getLastSensorValue
				router.get("/api/sensorValues/:sensorid").handler(this::getLastSensorValueFromSensorId);
				//getLastestSensorValue la diferencia es que tiene limite
				router.get("/api/sensorValues/:sensorid/:limit").handler(this::getLatestSensorValuesFromSensorId);
				//AddSensorValue
				router.post("/api/sensorValues").handler(this::addSensorValue);
				//DeleteSensorValue
				router.delete("/api/sensorValues/:sensorValueid").handler(this::deleteSensorValue);
		//-----------------------------Actuator Status-------------------------------------------------------------------
				//GetLastActuatorStatusFromActuatorId
				router.get("/api/actuatorStatus/:actuatorId").handler(this::GetLastActuatorStatusFromActuatorId);
				//AddSensorValue
				router.post("/api/actuatorStatus").handler(this::addActuatorStatus);
				//GetLatestActuatorStatesFromActuatorId
				router.get("/api/actuatorStatus/:actuatorid/:limit").handler(this::GetLatestActuatorStatesFromActuatorId);
				//DeleteActuatorStatus
				router.delete("/api/actuatorStatus/:actuatorid").handler(this::deleteActuatorStatus);
				
	}

	private DatabaseMessage deserializeDatabaseMessageFromMessageHandler(AsyncResult<Message<Object>> handler) {
		return gson.fromJson(handler.result().body().toString(), DatabaseMessage.class);
	}

	// --------------------------------------- GRUPOS -------------------------------------------
	private void getGrupoById(RoutingContext routingContext) {
		int groupId = Integer.parseInt(routingContext.request().getParam("groupid"));
		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.SELECT, DatabaseEntity.Group,
				DatabaseMethod.GetGroup, groupId);

		vertx.eventBus().request(RestEntityMessage.Group.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
						.end(gson.toJson(responseMessage.getResponseBodyAs(Group.class)));
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}
	
	private void addGroups(RoutingContext routingContext) {
		final Group grupo = gson.fromJson(routingContext.getBodyAsString(), Group.class);
		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.INSERT, DatabaseEntity.Group,
				DatabaseMethod.CreateGroup, gson.toJson(grupo));

		vertx.eventBus().request(RestEntityMessage.Group.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(201)
						.end(gson.toJson(responseMessage.getResponseBodyAs(Group.class)));
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}

	private void deleteGroup(RoutingContext routingContext) {
		int groupId = Integer.parseInt(routingContext.request().getParam("groupid"));

		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.DELETE, DatabaseEntity.Group,
				DatabaseMethod.DeleteGroup, groupId);

		vertx.eventBus().request(RestEntityMessage.Group.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
						.end(responseMessage.getResponseBody());
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}

	private void putGroup(RoutingContext routingContext) {
		final Group  grupo  = gson.fromJson(routingContext.getBodyAsString(), Group.class);
		int groupId = Integer.parseInt(routingContext.request().getParam("groupid"));
		grupo.setIdGroup(groupId);
		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.UPDATE, DatabaseEntity.Group,
				DatabaseMethod.EditGroup, gson.toJson(grupo ));

		vertx.eventBus().request(RestEntityMessage.Group.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(201)
						.end(gson.toJson(responseMessage.getResponseBodyAs(Sensor.class)));
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}

	private void GetDevicesFromGroupId(RoutingContext routingContext) {
		int groupId = Integer.parseInt(routingContext.request().getParam("groupid"));
		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.SELECT, DatabaseEntity.Device,
				DatabaseMethod.GetDevicesFromGroupId, groupId);

		vertx.eventBus().request(RestEntityMessage.Group.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
						.end(gson.toJson(responseMessage.getResponseBodyAs(Device[].class)));
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}
	
	// --------------------------------- DISPOSITIVOS --------------------------------------------

		private void getDeviceById(RoutingContext routingContext) {
			int deviceId = Integer.parseInt(routingContext.request().getParam("deviceid"));

			DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.SELECT, DatabaseEntity.Device,
					DatabaseMethod.GetDevice, deviceId);

			vertx.eventBus().request(RestEntityMessage.Device.getAddress(), gson.toJson(databaseMessage), handler -> {
				if (handler.succeeded()) {
					DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
							.end(gson.toJson(responseMessage.getResponseBodyAs(Device.class)));
				} else {
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
				}
			});
		}

		private void addDevice (RoutingContext routingContext) {
			final Device device = gson.fromJson(routingContext.getBodyAsString(), Device.class);
			DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.INSERT, DatabaseEntity.Device,
					DatabaseMethod.CreateDevice, gson.toJson(device));

			vertx.eventBus().request(RestEntityMessage.Device.getAddress(), gson.toJson(databaseMessage), handler -> {
				if (handler.succeeded()) {
					DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(201)
							.end(gson.toJson(responseMessage.getResponseBodyAs(Device.class)));
				} else {
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
				}
			});
		}

		private void deleteDevice(RoutingContext routingContext) {
			int deviceId = Integer.parseInt(routingContext.request().getParam("deviceid"));

			DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.DELETE, DatabaseEntity.Device,
					DatabaseMethod.DeleteDevice, deviceId);

			vertx.eventBus().request(RestEntityMessage.Device.getAddress(), gson.toJson(databaseMessage), handler -> {
				if (handler.succeeded()) {
					DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
							.end(responseMessage.getResponseBody());
				} else {
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
				}
			});
		}

		private void putDevice(RoutingContext routingContext) {
			final Device device = gson.fromJson(routingContext.getBodyAsString(), Device.class);
			int deviceId = Integer.parseInt(routingContext.request().getParam("deviceid"));
			device.setIdDevice(deviceId);
			DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.UPDATE, DatabaseEntity.Device,
					DatabaseMethod.EditDevice, gson.toJson(device));

			vertx.eventBus().request(RestEntityMessage.Device.getAddress(), gson.toJson(databaseMessage), handler -> {
				if (handler.succeeded()) {
					DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(201)
							.end(gson.toJson(responseMessage.getResponseBodyAs(Device.class)));
				} else {
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
				}
			});
		}
		
		private void getSensorsFromDeviceId (RoutingContext routingContext) {
			int deviceId = Integer.parseInt(routingContext.request().getParam("deviceid"));
			DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.SELECT, DatabaseEntity.Device,
					DatabaseMethod.GetSensorsFromDeviceId, deviceId);

			vertx.eventBus().request(RestEntityMessage.Device.getAddress(), gson.toJson(databaseMessage), handler -> {
				if (handler.succeeded()) {
					DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
							.end(gson.toJson(responseMessage.getResponseBodyAs(Sensor[].class)));
				} else {
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
				}
			});
		}
		
		private void getActuatorsFromDeviceId (RoutingContext routingContext) {
			int deviceId = Integer.parseInt(routingContext.request().getParam("deviceid"));

			DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.SELECT, DatabaseEntity.Device,
					DatabaseMethod.GetActuatorsFromDeviceId, deviceId);

			vertx.eventBus().request(RestEntityMessage.Device.getAddress(), gson.toJson(databaseMessage), handler -> {
				if (handler.succeeded()) {
					DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
							.end(gson.toJson(responseMessage.getResponseBodyAs(Actuator[].class)));
				} else {
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
				}
			});
		}
		
		private void GetSensorsFromDeviceIdAndSensorType (RoutingContext routingContext) {
			int deviceId = Integer.parseInt(routingContext.request().getParam("deviceid"));
			SensorType tipo = SensorType.valueOf(routingContext.request().getParam("sensortype"));
			DatabaseMessageIdAndSensorType data = new DatabaseMessageIdAndSensorType(deviceId, tipo);
			DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.SELECT, DatabaseEntity.Device,
					DatabaseMethod.GetSensorsFromDeviceIdAndSensorType, data);
			vertx.eventBus().request(RestEntityMessage.Device.getAddress(), gson.toJson(databaseMessage), handler -> {
				if (handler.succeeded()) {
					DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
							.end(gson.toJson(responseMessage.getResponseBodyAs(Sensor[].class)));
				} else {
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
				}
			});
		}
		
		
		private void GetActuatorsFromDeviceIdAndActuatorType (RoutingContext routingContext) {
			int deviceId = Integer.parseInt(routingContext.request().getParam("deviceid"));
			ActuatorType tipo = ActuatorType.valueOf(routingContext.request().getParam("actuatortype"));
			DatabaseMessageIdAndActuatorType data = new DatabaseMessageIdAndActuatorType(deviceId, tipo);
			DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.SELECT, DatabaseEntity.Device,
					DatabaseMethod.GetActuatorsFromDeviceIdAndActuatorType, data);
			vertx.eventBus().request(RestEntityMessage.Device.getAddress(), gson.toJson(databaseMessage), handler -> {
				if (handler.succeeded()) {
					DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
							.end(gson.toJson(responseMessage.getResponseBodyAs(Actuator[].class)));
				} else {
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
				}
			});
		}
		
		private void AddDeviceToGroup(RoutingContext routingContext) {
			final Device device = gson.fromJson(routingContext.getBodyAsString(), Device.class);
			int deviceId = Integer.parseInt(routingContext.request().getParam("deviceid"));
			int groupId = Integer.parseInt(routingContext.request().getParam("groupid"));
			device.setIdGroup(groupId);
			device.setIdDevice(deviceId);
			DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.UPDATE, DatabaseEntity.Device,
					DatabaseMethod.AddDeviceToGroup, gson.toJson(device));

			vertx.eventBus().request(RestEntityMessage.Group.getAddress(), gson.toJson(databaseMessage), handler -> {
				if (handler.succeeded()) {
					DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(201)
							.end(gson.toJson(responseMessage.getResponseBodyAs(Device.class)));
				} else {
					routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
				}
			});
		}
		
	
	// -------------------------------- SENSORES -------------------------------------------------
	private void getSensorById(RoutingContext routingContext) {
		int sensorId = Integer.parseInt(routingContext.request().getParam("sensorid"));

		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.SELECT, DatabaseEntity.Sensor,
				DatabaseMethod.GetSensor, sensorId);

		vertx.eventBus().request(RestEntityMessage.Sensor.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
						.end(gson.toJson(responseMessage.getResponseBodyAs(Sensor.class)));
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}

	private void addSensor(RoutingContext routingContext) {
		final Sensor sensor = gson.fromJson(routingContext.getBodyAsString(), Sensor.class);
		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.INSERT, DatabaseEntity.Sensor,
				DatabaseMethod.CreateSensor, gson.toJson(sensor));

		vertx.eventBus().request(RestEntityMessage.Sensor.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(201)
						.end(gson.toJson(responseMessage.getResponseBodyAs(Sensor.class)));
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}

	private void deleteSensor(RoutingContext routingContext) {
		int sensorId = Integer.parseInt(routingContext.request().getParam("sensorid"));

		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.DELETE, DatabaseEntity.Sensor,
				DatabaseMethod.DeleteSensor, sensorId);

		vertx.eventBus().request(RestEntityMessage.Sensor.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
						.end(responseMessage.getResponseBody());
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}

	private void putSensor(RoutingContext routingContext) {
		final Sensor sensor = gson.fromJson(routingContext.getBodyAsString(), Sensor.class);
		int sensorId = Integer.parseInt(routingContext.request().getParam("sensorid"));
		sensor.setIdSensor(sensorId);
		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.UPDATE, DatabaseEntity.Sensor,
				DatabaseMethod.EditSensor, gson.toJson(sensor));

		vertx.eventBus().request(RestEntityMessage.Sensor.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(201)
						.end(gson.toJson(responseMessage.getResponseBodyAs(Sensor.class)));
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}

	
	// -------------------------------- ACTUADORES ------------------------------------------------
	private void getActuatorById(RoutingContext routingContext) {
		int actuatorid = Integer.parseInt(routingContext.request().getParam("actuatorid"));

		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.SELECT, DatabaseEntity.Actuator,
				DatabaseMethod.GetActuator, actuatorid);

		vertx.eventBus().request(RestEntityMessage.Actuator.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
						.end(gson.toJson(responseMessage.getResponseBodyAs(Actuator.class)));
				
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}

	private void addActuator(RoutingContext routingContext) {
		final Actuator actuator = gson.fromJson(routingContext.getBodyAsString(), Actuator.class);
		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.INSERT, DatabaseEntity.Actuator,
				DatabaseMethod.CreateActuator, gson.toJson(actuator));

		vertx.eventBus().request(RestEntityMessage.Actuator.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(201)
						.end(gson.toJson(responseMessage.getResponseBodyAs(Actuator.class)));
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}

	private void deleteActuator(RoutingContext routingContext) {
		int actuatorid = Integer.parseInt(routingContext.request().getParam("actuatorid"));

		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.DELETE, DatabaseEntity.Actuator,
				DatabaseMethod.DeleteActuator, actuatorid);

		vertx.eventBus().request(RestEntityMessage.Actuator.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
						.end(responseMessage.getResponseBody());
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}

	private void putActuator(RoutingContext routingContext) {
		final Actuator actuator = gson.fromJson(routingContext.getBodyAsString(), Actuator.class);
		int actuatorId = Integer.parseInt(routingContext.request().getParam("actuatorid"));
		actuator.setIdActuator(actuatorId);
		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.UPDATE, DatabaseEntity.Actuator,
				DatabaseMethod.EditActuator, gson.toJson(actuator));

		vertx.eventBus().request(RestEntityMessage.Actuator.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(201)
						.end(gson.toJson(responseMessage.getResponseBodyAs(Actuator.class)));
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}

	//-------------------------------- SENSOR VALUES ----------------------------------------------
	private void addSensorValue(RoutingContext routingContext) {
		final SensorValue sensorValue = gson.fromJson(routingContext.getBodyAsString(),SensorValue.class);
		sensorValue.setTimestamp(Calendar.getInstance().getTimeInMillis());
		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.INSERT, DatabaseEntity.SensorValue,
				DatabaseMethod.CreateSensorValue, gson.toJson(sensorValue));

		vertx.eventBus().request(RestEntityMessage.SensorValue.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(201)
						.end(gson.toJson(responseMessage.getResponseBodyAs(SensorValue.class)));
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}
	
	private void deleteSensorValue(RoutingContext routingContext) {
		int sensorValueId = Integer.parseInt(routingContext.request().getParam("sensorValueid"));

		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.DELETE, DatabaseEntity.SensorValue,
				DatabaseMethod.DeleteSensorValue, sensorValueId);

		vertx.eventBus().request(RestEntityMessage.SensorValue.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
						.end(responseMessage.getResponseBody());
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}
	
	private void getLastSensorValueFromSensorId(RoutingContext routingContext) {
		int sensorValueId = Integer.parseInt(routingContext.request().getParam("sensorValueid"));
		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.SELECT, DatabaseEntity.SensorValue,
				DatabaseMethod.GetLastSensorValueFromSensorId, sensorValueId);

		vertx.eventBus().request(RestEntityMessage.SensorValue.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
						.end(gson.toJson(responseMessage.getResponseBodyAs(SensorValue.class)));
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}
	
	private void getLatestSensorValuesFromSensorId(RoutingContext routingContext) {
		int sensorValueId = Integer.parseInt(routingContext.request().getParam("sensorValueid"));
		int limit = Integer.parseInt(routingContext.request().getParam("limit"));
		DatabaseMessageLatestValues data = new DatabaseMessageLatestValues(sensorValueId,limit) ;
		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.SELECT, DatabaseEntity.SensorValue,
				DatabaseMethod.GetLatestSensorValuesFromSensorId, data);

		vertx.eventBus().request(RestEntityMessage.SensorValue.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
						.end(gson.toJson(responseMessage.getResponseBodyAs(SensorValue[].class)));
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}
	
	
	// ------------------------------ ACTUATOR STATUS --------------------------------------------
	
	private void addActuatorStatus(RoutingContext routingContext) {
		final ActuatorStatus sensorValue = gson.fromJson(routingContext.getBodyAsString(),ActuatorStatus.class);
		sensorValue.setTimestamp(Calendar.getInstance().getTimeInMillis());
		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.INSERT, DatabaseEntity.ActuatorStatus,
				DatabaseMethod.CreateActuatorStatus, gson.toJson(sensorValue));

		vertx.eventBus().request(RestEntityMessage.ActuatorStatus.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(201)
						.end(gson.toJson(responseMessage.getResponseBodyAs(ActuatorStatus.class)));
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}
	
	private void deleteActuatorStatus(RoutingContext routingContext) {
		int actuatorStatusId = Integer.parseInt(routingContext.request().getParam("actuatorStatusid"));

		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.DELETE, DatabaseEntity.ActuatorStatus,
				DatabaseMethod.DeleteActuatorStatus, actuatorStatusId);

		vertx.eventBus().request(RestEntityMessage.ActuatorStatus.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
						.end(responseMessage.getResponseBody());
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}
	
	private void GetLastActuatorStatusFromActuatorId(RoutingContext routingContext) {
		int ActuatorId = Integer.parseInt(routingContext.request().getParam("actuatorId"));
		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.SELECT, DatabaseEntity.ActuatorStatus,
				DatabaseMethod.GetLastActuatorStatusFromActuatorId, ActuatorId);

		vertx.eventBus().request(RestEntityMessage.ActuatorStatus.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
						.end(gson.toJson(responseMessage.getResponseBodyAs(ActuatorStatus.class)));
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}
	
	private void GetLatestActuatorStatesFromActuatorId(RoutingContext routingContext) {
		int ActuatorId = Integer.parseInt(routingContext.request().getParam("actuatorId"));
		int limit = Integer.parseInt(routingContext.request().getParam("limit"));
		DatabaseMessageLatestValues data = new DatabaseMessageLatestValues(ActuatorId,limit) ;
		DatabaseMessage databaseMessage = new DatabaseMessage(DatabaseMessageType.SELECT, DatabaseEntity.ActuatorStatus,
				DatabaseMethod.GetLatestActuatorStatesFromActuatorId, data);

		vertx.eventBus().request(RestEntityMessage.ActuatorStatus.getAddress(), gson.toJson(databaseMessage), handler -> {
			if (handler.succeeded()) {
				DatabaseMessage responseMessage = deserializeDatabaseMessageFromMessageHandler(handler);
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(200)
						.end(gson.toJson(responseMessage.getResponseBodyAs(ActuatorStatus[].class)));
			} else {
				routingContext.response().putHeader("content-type", "application/json").setStatusCode(500).end();
			}
		});
	}
	
	@Override
	public void stop(Future<Void> stopFuture) throws Exception {
		super.stop(stopFuture);
	}

}
