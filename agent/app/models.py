from typing import List, Literal

from pydantic import BaseModel, Field

Category = Literal["bad_deployment", "dependency_degradation", "traffic_surge", "upstream_outage", "unknown"]
ActionType = Literal["rollback", "scale_out", "circuit_break", "failover", "none"]


class Action(BaseModel):
    type: ActionType = "none"
    target: str = ""
    detail: str = ""


class Hypothesis(BaseModel):
    category: Category
    suspect_service: str = Field(description="Service where the root cause lives")
    summary: str = Field(description="One or two sentences naming the root cause")
    reasoning: str = Field(description="Short chain of reasoning over the cited evidence")
    confidence: float = Field(ge=0, le=1)
    citations: List[str] = Field(description="Evidence ids such as E1, E4 that support the claim")
    action: Action
