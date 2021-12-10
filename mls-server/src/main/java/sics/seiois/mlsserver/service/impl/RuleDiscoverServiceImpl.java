package sics.seiois.mlsserver.service.impl;

import com.sics.seiois.client.dto.ErrorCode;
import com.sics.seiois.client.dto.request.mls.RuleCompareRequest;
import com.sics.seiois.client.dto.request.mls.RuleDiscoverExecuteRequest;
import com.sics.seiois.client.dto.response.mls.RuleCompareResponse;
import com.sics.seiois.client.model.mls.TableInfos;
import com.sics.seiois.common.base.ErrorCodeEnum;
import com.sics.seiois.common.exception.AlertException;
import com.sics.seiois.common.httpbase.BaseResponse;
import com.sics.seiois.common.utils.ResponseUtil;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

import javax.annotation.Resource;

import sics.seiois.mlsserver.biz.validate.RuleCompareUtils;
import sics.seiois.mlsserver.dao.ModelDaoMapper;
import sics.seiois.mlsserver.enums.RuleFinderTypeEnum;
import sics.seiois.mlsserver.service.RuleDiscoverService;

@Service
public class RuleDiscoverServiceImpl implements RuleDiscoverService {

    private static final Logger logger = LoggerFactory.getLogger(RuleDiscoverServiceImpl.class);

    @Resource
    private ModelDaoMapper modelDaoMapper;

    @Override
    public BaseResponse execute(RuleDiscoverExecuteRequest request) {
        String taskId = request.getTaskId();
        TableInfos tableInfos = request.getTableInfos();

        //规则发现类型默认为30: er和cr同时发现
        if(request.getType() == null ||
                (!RuleFinderTypeEnum.CR_ONLY.getValue().equals(request.getType()) &&
                        !RuleFinderTypeEnum.ER_ONLY.getValue().equals(request.getType()))) {
            request.setType(RuleFinderTypeEnum.CR_AND_ER.getValue());
        }
        //如果有ER规则发现,必须要传标注集
        if(RuleFinderTypeEnum.ER_ONLY.getValue().equals(request.getType())) {
            //传入标注数据路径或者eidName为空，直接报错
            if(request.getErRuleConfig() == null ||
                    StringUtils.isEmpty(request.getErRuleConfig().getLabellingFilePath()) ||
                    StringUtils.isEmpty(request.getErRuleConfig().getEidName())) {
                logger.error("####{} rule discover execute param error. erRuleConfig error", taskId);
                return ResponseUtil.getErrorByCodeEnum(ErrorCodeEnum.INVALID_PARAM_ERROR);
            }
        }

//        StringBuffer otherParam = new StringBuffer();
//        otherParam.append("highSelectivityRatio=" + 0 + ";");
//        otherParam.append("interestingness=" + 1.5 + ";");
////        otherParam.append("queueName=" + queueName + ";");
//        otherParam.append("skipEnum=" + false + ";");
//        request.setOtherParam(otherParam.toString());

        //如果需要进行相似度规则发现，但是传入的相似度算法或者列信息都为空，提示错误
//        if(request.getSimilarityConfig() != null) {
//            if(request.getSimilarityConfig().getAlgorithms() == null || request.getSimilarityConfig().getAlgorithms().size() < 1 ||
//                    request.getSimilarityConfig().getColumns() == null || request.getSimilarityConfig().getColumns().size() < 1) {
//                logger.error("####{} rule discover execute param error. similarityConfig error", taskId);
//                return ResponseUtil.getErrorByCodeEnum(ErrorCodeEnum.INVALID_PARAM_ERROR);
//            }
//        }

        //TODO 模型的校验
//        for(int i = 0; i < tableInfos.getTableInfoList().size(); i++) {
//            List<ColumnInfo> columnList = request.getTableInfos().getTableInfoList().get(i).getColumnList();
//
//            if (null == columnList || columnList.size() < 1) {
//                logger.error("####{} rule discover execute param error. columnList is null", taskId);
//                return ResponseUtil.getErrorByCodeEnum(ErrorCodeEnum.INVALID_PARAM_ERROR);
//            }
//
//            //获取模型详细信息
//            for (ColumnInfo columnInfo : columnList) {
//                if (StringUtils.isNotEmpty(columnInfo.getModelId())) {
//                    Model model = modelDaoMapper.findById(columnInfo.getModelId());
//                    if (null == model) {
//                        logger.error("####{} rule discover execute not find model. modelId={}", taskId, columnInfo.getModelId());
//                        return ResponseUtil.getErrorByCodeEnum(ErrorCode.MLS_MODEL_NOT_FOUND);
//                    }
//                    columnInfo.setModelStorePath(model.getPath());
//                }
//            }
//        }

            //进行规则发现
            boolean executeSuccess = false;
            logger.info("####{} rule finder start.request={}", taskId, request);
            try {
                executeSuccess = sparkRuleDiscover(request);
            } catch (AlertException e) {
                logger.error("####{} rule discover execute call spark error! tableName={}", request.getTaskId(), taskId, e);
                return ResponseUtil.getErrorByCodeEnum(e);
            } catch (Exception e) {
                logger.error("####{} rule discover execute call spark error! tableName={}", request.getTaskId(), taskId, e);
                return ResponseUtil.getErrorByCodeEnum(ErrorCode.MLS_RULE_DISCOVER_EXECUTE_ERROR);
            }
            logger.info("####{} rule discover execute spark end.result={}", taskId, executeSuccess);
            if(!executeSuccess) {
                logger.warn("####{} rule discover execute call spark return failed! tableName={}", request.getTaskId(), taskId);
                return ResponseUtil.getErrorByCodeEnum(ErrorCode.MLS_RULE_DISCOVER_EXECUTE_RETURN_FAIL);
            }
        //}

        return ResponseUtil.getSuccessResponse();
    }

    private boolean sparkRuleDiscover(RuleDiscoverExecuteRequest request) {
        return RuleFinder.doRuleDiscovery(request);
    }

    /**
     * 规则对比
     * @param request
     * @return
     */
    @Override
    public BaseResponse compareRules(RuleCompareRequest request) {
        List<String> mainRules = request.getMainRule();
        List<String> compareRules = request.getCompareRule();
        if(request.getMainRulePath() != null) {
            if(request.getMainRulePath().endsWith(EvidenceGenerateMain.CSV_TYPE_SUFFIX)) {
                mainRules = RuleCompareUtils.readRuleFromFile(request.getMainRulePath());
            } else {
                mainRules = RuleCompareUtils.readRuleFromORCFile(request.getMainRulePath());
            }
        }

        if(request.getCompareRulePath() != null) {
            if(request.getCompareRulePath().endsWith(EvidenceGenerateMain.CSV_TYPE_SUFFIX)) {
                compareRules = RuleCompareUtils.readRuleFromFile(request.getCompareRulePath());
            } else {
                compareRules = RuleCompareUtils.readRuleFromORCFile(request.getCompareRulePath());
            }
        }

        if(mainRules == null || mainRules.size() == 0) {
            return ResponseUtil.getErrorByCodeEnum(ErrorCode.MLS_EMPTYRULE_ERROR);
        }
        if(compareRules == null || compareRules.size() == 0) {
            return ResponseUtil.getErrorByCodeEnum(ErrorCode.MLS_EMPTYRULE_ERROR);
        }

        List<String> differentRules = RuleCompareUtils.compareRule(mainRules, compareRules);
        RuleCompareResponse response = new RuleCompareResponse();
        response.setIsSame(true);

        if(differentRules.size() > 0) {
            response.setIsSame(false);
            response.setDifferentRules(differentRules);

            return ResponseUtil.getSuccessResponse(response);
        } else {
            differentRules = RuleCompareUtils.compareRule(compareRules, mainRules);

            if(differentRules.size() > 0) {
                response.setIsSame(false);
                response.setDifferentRules(differentRules);

                return ResponseUtil.getSuccessResponse(response);
            }
        }


        return ResponseUtil.getSuccessResponse(response);
    }

}
