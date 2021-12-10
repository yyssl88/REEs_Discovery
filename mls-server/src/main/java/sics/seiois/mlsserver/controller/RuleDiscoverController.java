package sics.seiois.mlsserver.controller;

import com.sics.seiois.client.dto.request.mls.RuleCompareRequest;
import com.sics.seiois.client.dto.request.mls.RuleDiscoverExecuteRequest;
import com.sics.seiois.common.httpbase.BaseResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import sics.seiois.mlsserver.service.RuleDiscoverService;

@RestController
@RequestMapping("/api/v1/mls/rulediscover")
@Api(value = "规则controller")
public class RuleDiscoverController {

    private final Log logger = LogFactory.getLog(RuleDiscoverController.class);

    @Autowired
    private RuleDiscoverService ruleDiscoverService;

    /**
     * 规则发现执行
     * @param request
     * @return
     */
    @ApiOperation(value="规则发现执行")
    @RequestMapping(value="/execute", method= RequestMethod.POST, consumes = "application/json")
    public BaseResponse executeRuleFinder(@RequestBody @Valid RuleDiscoverExecuteRequest request) {
        return ruleDiscoverService.execute(request);
    }

    /**
     * 规则对比
     * @param request
     * @return
     */
    @ApiOperation(value="规则对比")
    @RequestMapping(value="/comparerule", method= RequestMethod.POST, consumes = "application/json")
    public BaseResponse compareRules(@RequestBody @Valid RuleCompareRequest request) {
        return ruleDiscoverService.compareRules(request);
    }
}
