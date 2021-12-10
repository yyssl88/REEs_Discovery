package sics.seiois.mlsserver.service;

import com.sics.seiois.client.dto.request.mls.RuleCompareRequest;
import com.sics.seiois.client.dto.request.mls.RuleDiscoverExecuteRequest;
import com.sics.seiois.common.httpbase.BaseResponse;

public interface RuleDiscoverService {

    /**
     * 规则发现执行
     * @param request
     * @return
     */
    BaseResponse execute(RuleDiscoverExecuteRequest request);

    /**
     * 规则对比
     * @param request
     * @return
     */
    BaseResponse compareRules(RuleCompareRequest request);
}
