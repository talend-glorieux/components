/**
 * TComp Talend components JavaScript client
 *
 * @param prefix the url of the running talend component web service instance
 * @returns {Object}
 */
function TComp(prefix) {
	let urlPrefix = '/api';
	if (prefix) {
		urlPrefix = prefix;
	}

	const types = {
		datastore: 'datastore',
		dataset: 'dataset',
		transformer: 'transformer',
	};

	/**
	 * get
	 *
	 * @private
	 * @param url
	 * @returns {Promise}
	 */
	function get(url) {
		return fetch(urlPrefix + url)
			.then(function (response) {
				return response.json();
			});
	}

	/**
	 * post
	 *
	 * @private
	 * @param url
	 * @param body
	 * @returns {Promise}
	 */
	function post(url, body) {
		return fetch(urlPrefix + url, {
			method: 'POST',
			headers: {
				Accept: 'application/json',
				'Content-Type': 'application/json',
			},
			body: JSON.stringify(body),
		})
			.then(response => response.json());
	}

	// TODO: Remove once implementation is done
	function debugHandler(json, err) {
		if (json) {
			console.log(json);
		}
		if (err) {
			console.error(err);
		}
	}

	/**
	 * getComponents
	 *
	 * @private
	 * @param type the component's type
	 * @returns {Array.<string>}
	 */
	function getComponents(type) {
		//get(`/definitions/components?type=${type}`)
		return get('/components/names')
			.then(function (json) {
				return json['@items'];
			});
	}

	/**
	 * getProperties
	 *
	 * @private
	 * @param type
	 * @param componentName
	 * @returns {Object}
	 */
	function getProperties(type, componentName) {
		//get(`/definitions/${type}/${componentName}`)
		return get(`/components/properties/${componentName}`);
	}

	/**
	 * validateComponentProperties
	 *
	 * @private
	 * @param type
	 * @param name
	 * @param data
	 * @returns {Promise}
	 */
	function validateComponentProperties(type, name, data) {
		//post(`/definitions/${type}/${name}`, data)
		post(`/components/properties/${name}/validate`, data)
			.then(debugHandler);
	}

	/**
	 * checkComponentConnection
	 *
	 * @private
	 * @param type
	 * @param name
	 * @param data
	 * @returns {Promise}
	 */
	function checkComponentConnection(type, name, data) {
		post(`/runtimes/${type}/${name}`, data)
			.then(debugHandler);
	}

	/**
	 * newDatasetPayload
	 *
	 * @private
	 * @param datastoreProperties
	 * @param datasetProperties
	 * @returns {Object}
	 */
	function newDatasetPayload(datastoreProperties, datasetProperties) {
		return {
			'datastore-ui-spec': datastoreProperties,
			'dataset-ui-spec': datasetProperties,
		};
	}

	/**
	 * Datastores
	 *
	 * @returns {Promise} A chainable Promise containing the datastores as an
	 * Array
	 */
	function Datastores() {
		return getComponents(types.datastore);
	}

	/**
	 * DatastoreProperties
	 *
	 * @param name The name of the tageted Datastore
	 * @returns {Promise} A Promise containing the UI specifications
	 */
	function DatastoreProperties(name) {
		return getProperties(types.datastore, name);
	}

	/**
	 * ValidateDatastoreProperties
	 *
	 * @param name The name of the targeted Datastore
	 * @param data the Datastore properties data you want to validate
	 * @returns {Promise} A Promise containing the validation status
	 */
	function ValidateDatastoreProperties(name, data) {
		return validateComponentProperties(types.datastore, name, data);
	}

	/**
	 * CheckDataStoreConnection
	 *
	 * @param name The name of the targeted Datastore
	 * @param data the Datastore properties data used for connection
	 * @returns {Promise} A Promise containing the connection status
	 */
	function CheckDataStoreConnection(name, data) {
		return checkComponentConnection(types.datastore, name, data);
	}

	/**
	 * DatasetProperties
	 *
	 * @param name the name of the targed Dataset
	 * @param datastoreName the name of the Dataset's Datastore
	 * @param datastoreProperties the Datastore properties
	 * @returns {Promise}
	 */
	function DatasetProperties(name, datastoreName, datastoreProperties) {
		post(`/definitions/${types.datastore}/${name}/${types.dataset}`, datastoreProperties)
			.then(debugHandler);
	}

	/**
	 * ValidateDatasetProperties
	 *
	 * @param name
	 * @param datastoreProperties
	 * @param datasetProperties
	 * @returns {undefined}
	 */
	function ValidateDatasetProperties(name, datastoreProperties, datasetProperties) {
		return validateComponentProperties(types.dataset, newDatasetPayload(
			datastoreProperties,
			datasetProperties
		));
	}

	/**
	 * CheckDatasetConnection
	 *
	 * @param name
	 * @param datastoreProperties
	 * @param datasetProperties
	 * @returns {undefined}
	 */
	function CheckDatasetConnection(name, datastoreProperties, datasetProperties) {
		return checkComponentConnection(types.dataset, name, newDatasetPayload(
			datastoreProperties,
			datasetProperties
		));
	}

	/**
	 * DatasetSchema
	 *
	 * @param name
	 * @param datastoreProperties
	 * @param datasetProperties
	 * @returns {undefined}
	 */
	function DatasetSchema(name, datastoreProperties, datasetProperties) {
		post(`/definitions/${types.dataset}/${name}/schema`, newDatasetPayload(
			datastoreProperties,
			datasetProperties
		)).then(debugHandler);
	}

	/**
	 * DatasetContent
	 *
	 * @param name
	 * @param datastoreProperties
	 * @param datasetProperties
	 * @returns {undefined}
	 */
	function DatasetContent(name, datastoreProperties, datasetProperties) {
		post(`/runtimes/${types.dataset}/${name}/data`, newDatasetPayload(
			datastoreProperties,
			datasetProperties
		)).then(debugHandler);
	}

	/**
	 * Transformers
	 *
	 * @returns {undefined}
	 */
	function Transformers() {
		return getComponents(types.transformer);
	}

	/**
	 * ValidateProperties
	 *
	 * @param name
	 * @param data
	 * @returns {undefined}
	 */
	function ValidateProperties(name, data) {
		post(`/properties/${name}/validate`, data)
			.then(debugHandler);
	}

	return {
		Datastores,
		DatastoreProperties,
		ValidateDatastoreProperties,
		CheckDataStoreConnection,
		DatasetProperties,
		ValidateDatasetProperties,
		CheckDatasetConnection,
		DatasetSchema,
		DatasetContent,
		Transformers,
		ValidateProperties,
	};
}

exports.default = TComp;
