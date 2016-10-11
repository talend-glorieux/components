/*
 * Copyright (C) 2015 Google Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.api.component.runtime;

import java.util.Map;

/**
 * Runtime classes should implement this interface in case they provide flow/after variables
 */
public interface HasReturnValues {

    /**
     * Gets after variable values
     * Can be called after all records processed
     *
     * @return a map of the return values.
     */
    Map<String, Object> getAfterValues();
    
    /**
     * Gets flow variable values
     * Can be called after each record processed
     *
     * @return a map of the return values.
     */
    Map<String, Object> getFlowValues();
}
