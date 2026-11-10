# Role del job de Glue con minimo privilegio: lee raw/ y scripts/, escribe processed/ y tmp/.
data "aws_iam_policy_document" "glue_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "glue_permisos" {
  statement {
    sid       = "ListarSoloPrefijosDelPipeline"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.datalake.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*", "processed/*", "scripts/*", "tmp/*"]
    }
  }

  statement {
    sid     = "LeerRawYScripts"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.datalake.arn}/raw/*",
      "${aws_s3_bucket.datalake.arn}/scripts/*",
    ]
  }

  statement {
    sid     = "EscribirProcessedYTmp"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.datalake.arn}/processed/*",
      "${aws_s3_bucket.datalake.arn}/tmp/*",
    ]
  }

  statement {
    sid       = "LogsDeGlue"
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:*:*:log-group:/aws-glue/*"]
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.proyecto}-glue-role"
  description        = "Role para el job Glue ETL de precios"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_policy" "glue" {
  name   = "${var.proyecto}-glue-permisos"
  policy = data.aws_iam_policy_document.glue_permisos.json
}

resource "aws_iam_role_policy_attachment" "glue" {
  role       = aws_iam_role.glue.name
  policy_arn = aws_iam_policy.glue.arn
}
