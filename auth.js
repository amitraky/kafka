const HTTP = require("../helper/http");
const Config = require("../config/Config");
const HostMaster = require("../schemas/HostMaster");
const Kafka = require("../helper/Kafka");

const { logger } = require("../zlogger/factory");

const Authenticate = async (req, res, next) => {
  try {
    if (req?.query?.real_time_update) {
      req.auth = {
        _id: req?.query?.user_id,
      };
      let host_name = await HostMaster.findOne({
        customer_id: req?.query?.user_id,
        host_lan_ip: req?.params?.host_name,
      });
      req.params.ip = req?.params?.host_name;
      req.params = {
        ...req.params,
        host_name: host_name?.host_name,
      };
      next();
    } else {
      const bearerToken = req.headers["authorization"];

      if (typeof bearerToken == "undefined" || !bearerToken) {
        return res
          .status(401)
          .json({ message: "Authorization Header not found" });
      }

      const token = bearerToken?.split(" ")?.[1];

      if (typeof token == "undefined" || !token) {
        return res
          .status(401)
          .json({ message: "Authorization Token not found" });
      }
      const url = `${Config.SAAS_AUTH_SERVICE_BASE_URL}/api/v1/open/verify-token`;
      const headers = {
        Authorization: `Bearer ${token || ""}`,
        "Content-Type": `application/json`,
      };
      const body = {};
      let validity = await HTTP.makeRequest(url, "GET", body, headers);
      // console.log("check validity ", JSON.stringify(validity))
      req.auth = validity;
      next();
    }
  } catch (error) {
    logger.error(`Authentication error: ${error}`, { at: new Error() });
    return res.status(error.statusCode || 500).json({
      message: error.message || "Something went wrong.",
    });
  }
};

const AuthenticateNew = async (req, res, next) => {
  try {
    if (req?.query?.real_time_update) {
      req.auth = { _id: req?.query?.user_id };

      const hostData = await HostMaster.findOne({
        customer_id: req?.query?.user_id,
        hots_lan_ip: req?.params?.host_name,
      });

      req.params = {
        ...req.params,
        ip: req?.params?.host_name,
        host_name: hostData?.host_name,
      };
      return next();
    }

    // Handle Authorization for non-real-time updates
    const bearerToken = req.headers["authorization"];
    if (!bearerToken) {
      return res
        .status(401)
        .json({ message: "Authorization Header not found" });
    }

    const requestPayload = { action: "check-auth", token: bearerToken };

    try {
      const response = await Kafka.sendRequest(requestPayload, "auth-saas");

      if (response?.status === "success" && response?.data) {
        req.auth = response.data;
        return next();
      }

      return res.status(response?.status ? 401 : 500).json({
        message:
          response?.err_msg ||
          (response?.status ? "Something went wrong." : "JWT expired"),
      });
    } catch (error) {
      return res.status(error.statusCode || 500).json({
        message: error.message || "Something went wrong.",
      });
    }
  } catch (error) {
    logger.error(`Authentication error: ${error}`, { at: new Error() });
    return res.status(error.statusCode || 500).json({
      message: error.message || "Something went wrong.",
    });
  }
};

module.exports = {
  Authenticate,
  AuthenticateNew,
};
