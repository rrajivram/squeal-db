// A description of one step of a query plan, for EXPLAIN: what the step is,
// the details that distinguish it (keys, predicate, columns, ...), how many
// rows it is expected to produce when statistics know, and the steps that
// feed it. Built from the same Source tree a query would execute — each
// Source describes itself through `Source::plan` — so EXPLAIN shows the plan
// that would really run, not a separate model of it.
#[derive(Debug, Clone, PartialEq)]
pub struct PlanNode {
    pub name: String,
    pub detail: String,
    pub rows: Option<usize>,
    // The part this step plays for its parent ("build", "probe", ...), for
    // parents whose inputs are not interchangeable.
    pub role: Option<&'static str>,
    pub children: Vec<PlanNode>,
}

impl PlanNode {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            detail: String::new(),
            rows: None,
            role: None,
            children: vec![],
        }
    }

    pub fn detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = detail.into();
        self
    }

    pub fn rows(mut self, rows: Option<usize>) -> Self {
        self.rows = rows;
        self
    }

    pub fn child(mut self, child: PlanNode) -> Self {
        self.children.push(child);
        self
    }

    pub fn children(mut self, children: Vec<PlanNode>) -> Self {
        self.children.extend(children);
        self
    }

    pub fn with_role(mut self, role: &'static str) -> Self {
        self.role = Some(role);
        self
    }

    // One line per node, children indented under their parent:
    //   Projection id, val
    //     HashJoin Inner on left(id) = right(id)
    //       [build] TableScan t1 (~50000 rows)
    //       [probe] TableScan t2 (~50000 rows)
    pub fn render(&self) -> String {
        let mut lines = vec![];
        self.render_into(0, &mut lines);
        lines.join("\n")
    }

    fn render_into(&self, depth: usize, lines: &mut Vec<String>) {
        let mut line = "  ".repeat(depth);
        if let Some(role) = self.role {
            line.push_str(&format!("[{role}] "));
        }
        line.push_str(&self.name);
        if !self.detail.is_empty() {
            line.push(' ');
            line.push_str(&self.detail);
        }
        if let Some(rows) = self.rows {
            line.push_str(&format!(" (~{rows} rows)"));
        }
        lines.push(line);
        for c in &self.children {
            c.render_into(depth + 1, lines);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_indents_children_and_shows_roles_details_and_row_estimates() {
        let plan = PlanNode::new("Projection")
            .detail("id, val")
            .child(
                PlanNode::new("HashJoin")
                    .detail("Inner on left(id) = right(id)")
                    .child(PlanNode::new("TableScan").detail("t1").rows(Some(50)).with_role("build"))
                    .child(PlanNode::new("TableScan").detail("t2").with_role("probe")),
            );
        assert_eq!(
            plan.render(),
            "Projection id, val\n  HashJoin Inner on left(id) = right(id)\n    [build] TableScan t1 (~50 rows)\n    [probe] TableScan t2"
        );
    }

    #[test]
    fn test_a_bare_node_renders_as_its_name() {
        assert_eq!(PlanNode::new("Distinct").render(), "Distinct");
    }
}
