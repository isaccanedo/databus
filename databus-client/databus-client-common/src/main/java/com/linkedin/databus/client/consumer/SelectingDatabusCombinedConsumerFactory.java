package com.linkedin.databus.client.consumer;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.util.ArrayList;
import java.util.List;

import com.linkedin.databus.client.pub.DatabusBootstrapConsumer;
import com.linkedin.databus.client.pub.DatabusStreamConsumer;

public class SelectingDatabusCombinedConsumerFactory {

	static public List<SelectingDatabusCombinedConsumer> convertListOfStreamConsumers(List<DatabusStreamConsumer> dscs)
	{
		List<SelectingDatabusCombinedConsumer> lsdcc = new ArrayList<SelectingDatabusCombinedConsumer>();
		for (DatabusStreamConsumer dsc : dscs )
		{
			SelectingDatabusCombinedConsumer sdsc = new SelectingDatabusCombinedConsumer(dsc);
			lsdcc.add(sdsc);
		}
		return lsdcc;
	}
	
	static public List<SelectingDatabusCombinedConsumer> convertListOfBootstrapConsumers(List<DatabusBootstrapConsumer> dscs)
	{
		List<SelectingDatabusCombinedConsumer> lsdcc = new ArrayList<SelectingDatabusCombinedConsumer>();
		for (DatabusBootstrapConsumer dsc : dscs )
		{
			SelectingDatabusCombinedConsumer sdsc = new SelectingDatabusCombinedConsumer(dsc);
			lsdcc.add(sdsc);
		}
		return lsdcc;
	}

}
