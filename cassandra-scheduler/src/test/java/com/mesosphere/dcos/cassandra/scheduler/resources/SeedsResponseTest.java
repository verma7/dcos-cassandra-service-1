package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
//import org.codehaus.jackson.JsonNode;
import com.google.common.collect.Iterators;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SeedsResponseTest {
    @Before
    public void beforeEach() {

    }
    @Test
    public void testJSONSerializationWithSnakeCaseMembers() throws Exception {
        SeedsResponse response = SeedsResponse.create(true, Arrays.asList("seed1"));
        String jsonResponse = JsonUtils.toJsonString(response);
        ObjectMapper om = new ObjectMapper();

        JsonNode rehydratedResponse = om.readTree(jsonResponse);
        List<String> keys = new ArrayList<>();
        Iterators.addAll(keys, rehydratedResponse.fieldNames());
        keys.sort(String::compareTo);

        Assert.assertEquals(Arrays.asList("is_seed", "seeds"), keys);

        response = JsonUtils.MAPPER.readValue(jsonResponse, SeedsResponse.class);
        Assert.assertEquals(true, response.isSeed());
        Assert.assertEquals(Arrays.asList("seed1"), response.getSeeds());
    }
}