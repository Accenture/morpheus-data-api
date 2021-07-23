
variable "ami" {
  description = "AMI"
  type        = string
  default     = "ami-0e6f4c2b6023d32fb"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-north-1"
}

variable "instance_type" {
  description = "Instance Type"
  type        = string
  default     = "t2.micro"
}

variable "subnet_id" {
  description = "Subnet ID"
  type        = string
  default     = "subnet-0162643b6a646f87f"
}

variable "vpc_security_group_id" {
  description = "Security Group ID"
  type        = string
  default     = "sg-0ca967cb3cf95d01e"
}

resource "aws_instance" "blueprint1_spec1" {
  ami                   = var.ami
  instance_type         = var.vm_type
  subnet_id             = var.subnet_id
  vpc_security_group_id = var.vpc_security_group_id
  key_name              = var.key_name
  connection {
    host = aws_instance.blueprint1_spec1.public_ip
    user = "ubuntu"
  }
}